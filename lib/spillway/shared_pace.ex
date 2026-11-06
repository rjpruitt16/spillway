# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.SharedPace do
  @moduledoc """
  Holds a target's total rate across all of its account queues on this node.

  Each account queue still paces itself, but before it dispatches it must
  also take the next slot here, so the target sees at most its ceiling no
  matter how many tenants are active. Before, the ceiling was enforced only
  by UrlActor's budget check every 3 seconds, and each queue's per-success
  recovery climbed back to the full rate in between (aquifer#146; Aquifer's
  `internal/dispatch/shared_pace.go` is the same design).

  Slots go to queues in the order they started waiting, not to whichever asks
  first: a queue that wakes often (a steady stream of new jobs) would
  otherwise take nearly every slot from one with a static backlog that only
  wakes on its timer.

  One process per target domain, started by that domain's UrlActor and
  stopped when it goes down. A queue whose target has no pace process (a
  standalone queue, or one that outlived its UrlActor) is not limited here.
  """

  use GenServer

  @registry Spillway.SharedPaceRegistry
  @supervisor Spillway.SharedPaceSupervisor
  @min_rps 0.5
  # A queue that wakes a little late, or catches up in one dispatch batch,
  # may use up to this much unused time; idle time doesn't bank a burst.
  @max_credit_us 5_000
  # How long a pool's live capacity is reused before asking the pool again.
  @pool_refresh_us 250_000

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
  Sets the target's ceiling: a rate (requests/s) or `{:pool, pid}` for a
  pool's live capacity. Starts the pace process if needed. `owner` (the
  UrlActor) is monitored; the process stops when it goes down.
  """
  def set_ceiling(domain, ceiling, owner \\ self()) do
    case ensure_started(domain) do
      {:ok, pid} -> GenServer.cast(pid, {:ceiling, ceiling, owner})
      _error -> :ok
    end
  end

  @doc """
  Records the rate the target advertised (`X-Aqueduct-Rps`, or ORCA) on a
  successful response. It replaces a static ceiling until the next
  `set_ceiling/3`: in Spillway a header can raise a queue past its configured
  rate, so the total must follow it too. A pool's capacity is unaffected.
  """
  def advertise(domain, rps) when is_number(rps) do
    case Registry.lookup(@registry, domain) do
      [{pid, _}] -> GenServer.cast(pid, {:advertised, rps})
      [] -> :ok
    end
  end

  @doc """
  Takes the next slot for `who` if one is free and it's who's turn, and
  returns 0. Otherwise takes nothing and returns how many milliseconds to
  wait before asking again. Never limits when the target has no pace process.
  """
  def reserve(domain, who \\ self()) do
    case Registry.lookup(@registry, domain) do
      [{pid, _}] -> GenServer.call(pid, {:reserve, who}, 1_000)
      [] -> 0
    end
  catch
    :exit, _reason -> 0
  end

  @doc "The target's current ceiling (requests/s), or nil with no pace process."
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
       advertised: nil,
       owner: nil,
       pool_rate: 0.0,
       pool_read_at: nil,
       last: nil,
       waiting: %{}
     }}
  end

  @impl true
  def handle_cast({:ceiling, ceiling, owner}, state) do
    state =
      if state.owner == owner do
        state
      else
        Process.monitor(owner)
        %{state | owner: owner}
      end

    {:noreply, %{state | ceiling: ceiling, advertised: nil, pool_read_at: nil}}
  end

  def handle_cast({:advertised, rps}, state), do: {:noreply, %{state | advertised: rps}}

  @impl true
  def handle_call({:reserve, who}, _from, state) do
    now = System.monotonic_time(:microsecond)
    {rate, state} = current_rate(state, now)

    if rate <= 0 do
      {:reply, 0, state}
    else
      {reply, state} = take_slot(state, who, max(rate, @min_rps), now)
      {:reply, reply, state}
    end
  end

  def handle_call(:rate, _from, state) do
    {rate, state} = current_rate(state, System.monotonic_time(:microsecond))
    {:reply, rate, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{owner: pid} = state),
    do: {:stop, :normal, state}

  def handle_info(_msg, state), do: {:noreply, state}

  defp take_slot(state, who, rate, now) do
    interval = round(1_000_000 / rate)
    # Forget queues that stopped asking. A waiting queue asks again at least
    # once per interval (its timer is set to the wait returned here).
    stale = 2 * interval + 100_000
    waiting = Map.reject(state.waiting, fn {_who, {_since, asked}} -> now - asked > stale end)
    state = %{state | waiting: waiting}
    next = state.last && state.last + interval

    cond do
      next != nil and now < next ->
        {to_ms(next - now), mark_waiting(state, who, now)}

      older_waiter?(waiting, who) ->
        # The slot is free but belongs to a queue that has waited longer; ask
        # again shortly, after it has had the chance to take it.
        {to_ms(min(interval, 5_000)), mark_waiting(state, who, now)}

      true ->
        start = now - max(interval, @max_credit_us)
        start = if next != nil and next > start, do: next, else: start
        {0, %{state | last: start, waiting: Map.delete(waiting, who)}}
    end
  end

  defp older_waiter?(waiting, who) do
    case Map.fetch(waiting, who) do
      {:ok, {mine, _}} -> Enum.any?(waiting, fn {k, {since, _}} -> k != who and since < mine end)
      :error -> map_size(waiting) > 0
    end
  end

  defp mark_waiting(state, who, now) do
    since =
      case Map.fetch(state.waiting, who) do
        {:ok, {since, _}} -> since
        :error -> now
      end

    %{state | waiting: Map.put(state.waiting, who, {since, now})}
  end

  defp to_ms(us), do: max(div(us + 999, 1_000), 1)

  defp current_rate(%{ceiling: {:pool, pid}} = state, now) do
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

  defp current_rate(%{ceiling: rate} = state, _now) when is_number(rate),
    do: {state.advertised || rate, state}

  defp current_rate(state, _now), do: {0, state}
end
