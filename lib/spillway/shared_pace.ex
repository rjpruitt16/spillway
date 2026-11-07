# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.SharedPace do
  @moduledoc """
  Holds a target's total rate across all of its account queues on this node,
  and splits it between them by the rate the target advertises to each.

  Each account queue still paces itself, but before it dispatches it must
  also take the next slot here, so the target sees at most its total no
  matter how many tenants are active (aquifer#146). Aquifer's
  `internal/dispatch/shared_pace.go` is the same design.

  The target sets the total at runtime: the highest `X-Aqueduct-Rps` it is
  advertising to any queue that asked in the last 3 seconds. Only when it
  advertises nothing does the configured rate (or a pool's live capacity,
  which its members advertise) apply. A target that stamps one number on
  every response therefore gets that number in total, not per tenant.

  When queues contend, slots are shared in proportion to each queue's weight:
  the rate the target last advertised to it, or its own pace. Advertising 600
  to one tenant, 60 to another and 6 to a third gives them 90%, 9% and 1%;
  equal rates take equal turns. This is stride scheduling: taking a slot adds
  1/weight to a queue's pass, and a free slot goes to the contending queue
  with the lowest pass, ties to the one that has waited longest. A queue that
  wakes often can't take slots from one with a static backlog that only wakes
  on its timer, and a queue that was idle rejoins at the current pass instead
  of with banked turns.

  A queue contends from when it is turned away until it stops asking, and,
  after taking a slot, from when its own pace lets it ask again, for about
  its timer's jitter: a backlogged queue keeps its turn while it waits out its
  own interval instead of the slot going to whoever asks just before its
  timer fires. The queue counts its own pace from the slot it was given, not
  from when it dispatched, so the jitter doesn't add up and push it behind
  the slot schedule.

  One process per target domain, started by that domain's UrlActor and
  stopped when it goes down. A queue whose target has no pace process (a
  standalone queue, or one that outlived its UrlActor) is not limited here.
  """

  use GenServer

  @registry Spillway.SharedPaceRegistry
  @supervisor Spillway.SharedPaceSupervisor
  @min_rps 0.5
  # How long a pool's live capacity is reused before asking the pool again.
  @pool_refresh_us 250_000
  # How long a queue's advertised rate counts toward the total after it last
  # asked for a slot.
  @active_us 3_000_000
  # How long a queue that stopped asking keeps its pass.
  @forget_us 60_000_000

  def child_spec(domain) do
    %{
      id: {__MODULE__, domain},
      start: {__MODULE__, :start_link, [domain]},
      restart: :temporary
    }
  end

  def start_link(domain),
    do: GenServer.start_link(__MODULE__, domain, name: {:via, Registry, {@registry, domain}})

  @doc """
  Sets the target's configured ceiling: a rate (requests/s) or
  `{:pool, pid}` for a pool's live capacity. Starts the pace process if
  needed. `owner` (the UrlActor) is monitored; the process stops when it
  goes down.
  """
  def set_ceiling(domain, ceiling, owner \\ self()), do: set_ceiling(domain, ceiling, owner, 3)

  # Synchronous, and retried: a pace process can be exiting because its
  # previous owner went down just as a new UrlActor sets the ceiling, and a
  # cast to it would be lost, leaving the domain without a shared limit.
  defp set_ceiling(_domain, _ceiling, _owner, 0), do: :ok

  defp set_ceiling(domain, ceiling, owner, tries) do
    with {:ok, pid} <- ensure_started(domain) do
      GenServer.call(pid, {:ceiling, ceiling, owner}, 1_000)
    end
  catch
    :exit, _reason -> set_ceiling(domain, ceiling, owner, tries - 1)
  end

  @doc """
  Takes the next slot for `who` if one is free and it's who's turn.

  `weight` is who's share when queues contend (the rate the target advertised
  to it, or its own rate), `advertised` the rate the target advertised to it
  or nil, and `own_us` its own interval between dispatches. Returns
  `{:ok, age_us}`, how long ago the slot was scheduled (at most one interval;
  the queue counts its own pace from then), or `{:wait, ms}`. Never limits
  when the target has no pace process.
  """
  def reserve_slot(domain, who, weight, advertised, own_us) do
    case Registry.lookup(@registry, domain) do
      [{pid, _}] -> GenServer.call(pid, {:reserve, who, weight, advertised, own_us}, 1_000)
      [] -> {:ok, 0}
    end
  catch
    :exit, _reason -> {:ok, 0}
  end

  @doc "`reserve_slot/5` with equal weights; returns 0 or the wait in ms."
  def reserve(domain, who \\ self()) do
    case reserve_slot(domain, who, 1.0, nil, 0) do
      {:ok, _age} -> 0
      {:wait, ms} -> ms
    end
  end

  @doc "The target's current total (requests/s), or nil with no pace process."
  def rate(domain) do
    case Registry.lookup(@registry, domain) do
      [{pid, _}] -> GenServer.call(pid, :rate, 1_000)
      [] -> nil
    end
  catch
    :exit, _reason -> nil
  end

  defp ensure_started(domain) do
    case DynamicSupervisor.start_child(@supervisor, {__MODULE__, domain}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  catch
    :exit, reason -> {:error, reason}
  end

  # ---- GenServer ----

  @impl true
  def init(domain) do
    {:ok,
     %{
       domain: domain,
       ceiling: nil,
       owner: nil,
       owner_ref: nil,
       pool_rate: 0.0,
       pool_read_at: nil,
       last: nil,
       vtime: 0.0,
       queues: %{},
       # The queues whose window hasn't closed, the only ones a reservation
       # looks at: scanning thousands of recently drained queues on every
       # reservation would cost real CPU. Idle ones are swept out of :queues
       # at most once a second.
       contenders: MapSet.new(),
       swept: nil,
       # The highest rate advertised to a queue active in the last
       # @active_us, and that queue; recomputed only when it lowers its rate
       # or goes quiet.
       top: nil,
       top_who: nil
     }}
  end

  @impl true
  def handle_call({:ceiling, ceiling, owner}, _from, state) do
    state =
      if state.owner == owner do
        state
      else
        # A new UrlActor took over: stop watching the old one.
        if state.owner_ref, do: Process.demonitor(state.owner_ref, [:flush])
        %{state | owner: owner, owner_ref: Process.monitor(owner)}
      end

    {:reply, :ok, %{state | ceiling: ceiling, pool_read_at: nil}}
  end

  def handle_call({:reserve, who, weight, advertised, own_us}, _from, state) do
    now = System.monotonic_time(:microsecond)
    state = sweep(state, now)
    me = Map.get(state.queues, who, new_queue())
    me = if now > me.until, do: %{me | waiting: false}, else: me
    me = %{me | asked: now, advertised: positive(advertised)}
    state = note_advertised(%{state | queues: Map.put(state.queues, who, me)}, who, me, now)
    {rate, state} = total(state, now)

    if rate <= 0 do
      {:reply, {:ok, 0}, state}
    else
      weight = max(positive(weight) || @min_rps, @min_rps)
      {reply, state} = take_slot(state, who, weight, own_us, max(rate, @min_rps), now)
      {:reply, reply, state}
    end
  end

  def handle_call(:rate, _from, state) do
    {rate, state} = total(state, System.monotonic_time(:microsecond))
    {:reply, rate, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{owner: pid} = state),
    do: {:stop, :normal, state}

  def handle_info(_msg, state), do: {:noreply, state}

  defp new_queue,
    do: %{
      pass: 0.0,
      stride: 0.0,
      advertised: nil,
      waiting: false,
      since: 0,
      asked: 0,
      ready_at: 0,
      until: 0
    }

  defp positive(n) when is_number(n) and n > 0, do: n
  defp positive(_), do: nil

  defp take_slot(state, who, weight, own_us, rate, now) do
    interval = round(1_000_000 / rate)
    # A queue turned away asks again at least once per interval (its timer is
    # set to the wait returned here); one that doesn't has stopped.
    stale = 2 * interval + 100_000

    state = drop_closed_contenders(state, now)

    # The step this queue's last slot added to its pass was for its weight
    # then. If the target has since changed its rate, redo that step at the
    # new weight: a tenant promoted from 0.5 to 600 would otherwise wait out
    # a 2-unit step, about 1,200 slots of a queue at 600, before its first
    # turn at the new rate.
    stride = 1 / weight
    me = state.queues[who]

    me =
      if me.stride > 0 and me.stride != stride,
        do: %{me | pass: me.pass + stride - me.stride, stride: stride},
        else: me

    me = %{me | pass: max(me.pass, state.vtime)}
    state = %{state | queues: Map.put(state.queues, who, me)}
    next = state.last && state.last + interval

    cond do
      next != nil and now < next ->
        {{:wait, to_ms(next - now)}, turned_away(state, who, now, stale)}

      someone_first?(state, who, me, now) ->
        # The slot is free but belongs to a queue whose turn comes first; ask
        # again shortly, after it has had the chance to take it.
        {{:wait, to_ms(min(interval, 5_000))}, turned_away(state, who, now, stale)}

      true ->
        # Carry at most one interval of unused credit, so a timer that fires
        # a little late doesn't lose throughput, but idle time doesn't bank a
        # burst.
        start = now - interval
        start = if next != nil and next > start, do: next, else: start
        # It stays in line for its next turn, around when its own pace lets
        # it ask again.
        hold = min(div(own_us, 4) + 2_000, interval)

        me = %{
          me
          | pass: me.pass + stride,
            stride: stride,
            waiting: false,
            since: now,
            ready_at: start + own_us,
            until: start + own_us + hold
        }

        {{:ok, now - start},
         %{
           state
           | last: start,
             vtime: state.queues[who].pass,
             queues: Map.put(state.queues, who, me),
             contenders: MapSet.put(state.contenders, who)
         }}
    end
  end

  defp turned_away(state, who, now, stale) do
    q = state.queues[who]
    q = if q.waiting, do: q, else: %{q | waiting: true, since: now}

    %{
      state
      | queues: Map.put(state.queues, who, %{q | ready_at: now, until: now + stale}),
        contenders: MapSet.put(state.contenders, who)
    }
  end

  # Contenders whose window closed stop contending.
  defp drop_closed_contenders(state, now) do
    {open, closed} =
      Enum.split_with(state.contenders, fn k ->
        q = state.queues[k]
        q != nil and now <= q.until
      end)

    queues =
      Enum.reduce(closed, state.queues, fn k, queues ->
        case queues do
          %{^k => q} -> Map.put(queues, k, %{q | waiting: false})
          _ -> queues
        end
      end)

    %{state | contenders: MapSet.new(open), queues: queues}
  end

  # Forgets queues that stopped asking, at most once a second.
  defp sweep(%{swept: swept} = state, now) when is_integer(swept) and now - swept < 1_000_000,
    do: state

  defp sweep(state, now) do
    queues = Map.reject(state.queues, fn {_k, q} -> now - q.asked > @forget_us end)

    %{
      state
      | swept: now,
        queues: queues,
        contenders: MapSet.filter(state.contenders, &Map.has_key?(queues, &1))
    }
  end

  defp note_advertised(state, who, me, now) do
    cond do
      me.advertised != nil and (state.top == nil or me.advertised >= state.top) ->
        %{state | top: me.advertised, top_who: who}

      state.top_who == who ->
        # The top queue lowered its rate or stopped advertising.
        recompute_top(state, now)

      true ->
        state
    end
  end

  defp recompute_top(state, now) do
    {top, who} =
      Enum.reduce(state.queues, {nil, nil}, fn {k, q}, {top, who} ->
        if q.advertised && now - q.asked <= @active_us && (top == nil or q.advertised > top),
          do: {q.advertised, k},
          else: {top, who}
      end)

    %{state | top: top, top_who: who}
  end

  defp someone_first?(state, who, me, now) do
    Enum.any?(state.contenders, fn k ->
      q = state.queues[k]

      if k == who or now > q.until or now < q.ready_at do
        false
      else
        # A contender's pass may have fallen behind vtime while it waited for
        # its timer; it rejoins at vtime, like anyone else.
        pass = max(q.pass, state.vtime)
        pass < me.pass or (pass == me.pass and (not me.waiting or q.since < me.since))
      end
    end)
  end

  defp to_ms(us), do: max(div(us + 999, 1_000), 1)

  # The highest rate advertised to a queue active in the last @active_us,
  # else the configured ceiling. A pool's total is its live capacity.
  defp total(%{ceiling: {:pool, pid}} = state, now) do
    if state.pool_read_at && now - state.pool_read_at < @pool_refresh_us do
      {state.pool_rate, state}
    else
      rate =
        try do
          Spillway.Pool.total_capacity(pid)
        catch
          :exit, _reason -> state.pool_rate
        end

      {rate, %{state | pool_rate: rate, pool_read_at: now}}
    end
  end

  defp total(state, now) do
    state =
      case state.queues[state.top_who] do
        %{asked: asked} when now - asked <= @active_us -> state
        _ when state.top_who != nil -> recompute_top(state, now)
        _ -> state
      end

    cond do
      state.top != nil -> {state.top, state}
      is_number(state.ceiling) -> {state.ceiling, state}
      true -> {0, state}
    end
  end
end
