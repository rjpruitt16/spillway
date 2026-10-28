# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.InboundRate do
  @moduledoc """
  Decides the X-Aqueduct-Rps Spillway advertises to its own callers (a
  gateway like Canalis, another Spillway or Aquifer, any client that honors
  the header): the server side of pacing. Mirrors Aquifer's
  InboundRateController, constants included.

  The expensive part of a submission is the durable insert, so the
  controller watches inserts per second and insert latency:

    * it starts at SPILLWAY_INBOUND_START_PERCENT (default 40) of
      SPILLWAY_INBOUND_BENCHMARK_RPS (default 400 inserts/s);
    * it cuts 30% when p95 insert latency rises well above its healthy
      baseline, when the backlog keeps growing, or when the disk falls
      behind (store sync lag over 500ms), at most once per 3s so callers
      (a round trip behind) can react before it cuts again;
    * each cut remembers a threshold just under the failing rate (TCP's
      ssthresh); the benchmark is the initial threshold;
    * it raises only while healthy and callers are using at least 80% of
      the rate: 1.5x per second below the threshold, 5% above it.

  Each active caller (user_id seen in the last 3 seconds) is advertised an
  equal share of the total. Always on: a caller that ignores the header
  loses nothing.

  Submissions are recorded in a public ETS table, so the request path never
  waits on this process; it reads and resets them once a second.
  """

  use GenServer

  @table :spillway_inbound_rate
  @tick_ms 1_000
  @caller_ttl_ms 3_000
  @min_samples 20
  @latency_slack_us 2_000
  @max_samples 8192
  @cut 0.7
  @fast_growth 1.5
  @careful_growth 1.05
  @cut_cooldown_ticks 3
  # Latency lags a cut by however long admitted work takes to drain, so
  # within one congestion episode cut once, then hold, cutting again only if
  # the episode outlasts this many ticks.
  @episode_recut_ticks 10
  # Backlog growing on this many consecutive ticks means accepted work is
  # arriving faster than it can be dispatched.
  @backlog_growth_ticks 3
  @overshoot_factor 1.5
  @used_fraction 0.8
  # The disk is behind once acknowledged inserts have gone this long
  # unsynced: several of the default 100ms syncs missed. Admission refuses
  # new jobs at SPILLWAY_MAX_SYNC_LAG_MS (1s); slowing callers first keeps
  # the lag from getting there. A longer SPILLWAY_STORE_SYNC_INTERVAL_MS
  # raises it to two intervals, so a healthy disk between syncs isn't
  # mistaken for one falling behind.
  @sync_lag_limit_ms 500

  defstruct [
    :benchmark,
    :floor,
    :ceiling,
    :rate,
    :threshold,
    sync_lag_limit_ms: @sync_lag_limit_ms,
    cooldown: 0,
    # True from the first cut until healthy again; only that first cut sets
    # the threshold, so a cascade of cuts while admitted work drains doesn't
    # drag the learned knee down with it.
    congested: false,
    episode: 0,
    prev_backlog: -1,
    backlog_grows: 0,
    baseline_us: 0,
    cuts: 0,
    raises: 0,
    last: %{}
  ]

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @doc "Records one submission: who sent it, how long the durable insert took, and whether it inserted a new job."
  def observe(user_id, latency_us, inserted) do
    touch(user_id)
    if inserted, do: :ets.update_counter(@table, :inserts, {2, 1}, {:inserts, 0})

    if :ets.update_counter(@table, :samples, {2, 1}, {:samples, 0}) <= @max_samples,
      do: :ets.insert(@table, {{:latency, :erlang.unique_integer()}, latency_us})

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "The X-Aqueduct-Rps to send user_id: its share of the total, or nil before the controller starts."
  def advertised_rps(user_id) do
    touch(user_id)

    case :ets.lookup(@table, :rate) do
      [{:rate, rate}] -> rate / max(active_callers(System.monotonic_time(:millisecond)), 1)
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc "The last tick's state, for GET /health."
  def snapshot do
    case :ets.lookup(@table, :snapshot) do
      [{:snapshot, snapshot}] -> snapshot
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  defp touch(user_id) when is_binary(user_id) and user_id != "",
    do: :ets.insert(@table, {{:caller, user_id}, System.monotonic_time(:millisecond)})

  defp touch(_user_id), do: :ok

  defp active_callers(now) do
    :ets.select_count(@table, [
      {{{:caller, :_}, :"$1"}, [{:>=, :"$1", now - @caller_ttl_ms}], [true]}
    ])
  end

  # ---- GenServer ----

  @impl true
  def init(:ok) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      write_concurrency: true,
      read_concurrency: true
    ])

    state =
      new(
        env_float("SPILLWAY_INBOUND_BENCHMARK_RPS", 400),
        env_float("SPILLWAY_INBOUND_START_PERCENT", 40),
        Spillway.Store.sync_interval_ms()
      )

    publish(state)
    Process.send_after(self(), :tick, @tick_ms)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, @tick_ms)
    now = System.monotonic_time(:millisecond)
    inserts = take_counter(:inserts)
    samples = take_samples()

    :ets.select_delete(@table, [
      {{{:caller, :_}, :"$1"}, [{:<, :"$1", now - @caller_ttl_ms}], [true]}
    ])

    state =
      tick(state, %{
        inserts: inserts,
        latencies_us: samples,
        backlog: backlog(),
        sync_lag_ms: sync_lag_ms(),
        active_callers: active_callers(now)
      })

    publish(state)
    {:noreply, state}
  end

  defp publish(state) do
    :ets.insert(@table, [{:rate, state.rate}, {:snapshot, state.last}])
  end

  defp take_counter(key) do
    case :ets.take(@table, key) do
      [{^key, n}] -> n
      [] -> 0
    end
  end

  defp take_samples do
    samples = :ets.select(@table, [{{{:latency, :_}, :"$1"}, [], [:"$1"]}])
    :ets.select_delete(@table, [{{{:latency, :_}, :_}, [], [true]}])
    take_counter(:samples)
    samples
  end

  # Queued and in-flight work on this node: insert latency only sees the
  # cost of accepting jobs, and when dispatch falls behind the backlog grows
  # while inserts stay fast.
  defp backlog do
    Spillway.AccountQueue.local_backlogs() |> Enum.map(&elem(&1, 1)) |> Enum.sum()
  end

  defp sync_lag_ms do
    Application.get_env(:spillway, :inbound_sync_lag_reader, &Spillway.Store.sync_lag_ms/0).()
  end

  defp env_float(name, fallback) do
    case Float.parse(System.get_env(name, "")) do
      {value, ""} when value > 0 -> value
      _ -> fallback
    end
  end

  # ---- The controller, as pure functions ----

  @doc false
  def new(benchmark, start_percent, sync_interval_ms \\ 100) do
    benchmark = if benchmark > 0, do: benchmark * 1.0, else: 400.0
    floor = 1.0
    rate = max(benchmark * start_percent / 100, floor)

    state = %__MODULE__{
      benchmark: benchmark,
      floor: floor,
      ceiling: benchmark * 10,
      rate: rate,
      threshold: benchmark,
      sync_lag_limit_ms: max(@sync_lag_limit_ms, 2 * sync_interval_ms)
    }

    %{state | last: snapshot_of(state, %{inserts: 0, p95_us: 0, active_callers: 0}, "start")}
  end

  @doc """
  One second's decision. `observed` has the second's inserts, insert
  latencies (microseconds), the node's backlog, the store's sync lag and the
  number of active callers.
  """
  def tick(%__MODULE__{} = c, observed) do
    %{inserts: inserts, latencies_us: samples, backlog: backlog, sync_lag_ms: sync_lag} =
      observed

    p95 = percentile(samples, 0.95)
    enough = length(samples) >= @min_samples

    baseline =
      cond do
        not enough -> c.baseline_us
        c.baseline_us == 0 or p95 < c.baseline_us -> p95
        # Drift up slowly so a stale best-case minimum (or slower hardware
        # than at startup) can't pin the controller low forever.
        true -> c.baseline_us * 0.99 + p95 * 0.01
      end

    latency_healthy = not enough or p95 <= baseline * 1.5 + @latency_slack_us

    backlog_growing =
      c.prev_backlog >= 0 and backlog >= 0 and
        backlog - c.prev_backlog > max(10, 0.1 * inserts)

    backlog_grows = if backlog_growing, do: c.backlog_grows + 1, else: 0
    backlog_overloaded = backlog_grows >= @backlog_growth_ticks
    disk_behind = sync_lag > c.sync_lag_limit_ms

    healthy = latency_healthy and not backlog_overloaded and not disk_behind

    cut_reason =
      cond do
        not latency_healthy -> "cut: insert latency"
        disk_behind -> "cut: disk behind"
        true -> "cut: backlog growing"
      end

    overshoot = inserts > c.rate * @overshoot_factor
    used = inserts >= c.rate * @used_fraction

    c = %{c | baseline_us: baseline, prev_backlog: backlog, backlog_grows: backlog_grows}
    c = if healthy, do: %{c | congested: false}, else: c
    c = if c.cooldown > 0, do: %{c | cooldown: c.cooldown - 1}, else: c
    c = if c.congested, do: %{c | episode: c.episode + 1}, else: c

    {c, adjustment} =
      cond do
        not healthy and (not c.congested or c.episode >= @episode_recut_ticks) and
            c.cooldown == 0 ->
          c =
            if c.congested,
              do: c,
              else: %{c | threshold: max(c.rate * 0.9, c.floor), congested: true}

          {%{
             c
             | episode: 0,
               rate: max(c.rate * @cut, c.floor),
               cooldown: @cut_cooldown_ticks,
               cuts: c.cuts + 1
           }, cut_reason}

        not healthy ->
          {c, "hold: waiting for last cut"}

        backlog_growing ->
          {c, "hold: backlog growing"}

        # Callers over their share are admission control's problem; cutting
        # here spirals to the floor whenever callers lag a rate change.
        overshoot ->
          {c, "hold: callers over advertised rate"}

        used ->
          growth = if c.rate >= c.threshold, do: @careful_growth, else: @fast_growth

          {%{c | congested: false, rate: min(c.rate * growth, c.ceiling), raises: c.raises + 1},
           "raise"}

        true ->
          {c, "hold"}
      end

    last =
      c
      |> snapshot_of(Map.put(observed, :p95_us, p95), adjustment)
      |> Map.merge(%{server_backlog: max(backlog, 0), sync_lag_ms: round2(sync_lag)})

    %{c | last: last}
  end

  defp snapshot_of(c, observed, adjustment) do
    active = Map.get(observed, :active_callers, 0)

    %{
      advertised_rps: round2(c.rate),
      per_caller_rps: round2(c.rate / max(active, 1)),
      active_callers: active,
      inserts_per_sec: observed.inserts,
      p95_insert_ms: round2(observed.p95_us / 1000),
      baseline_insert_ms: round2(c.baseline_us / 1000),
      benchmark_rps: c.benchmark,
      threshold_rps: round2(c.threshold),
      cuts: c.cuts,
      raises: c.raises,
      last_adjustment: adjustment
    }
  end

  defp percentile([], _q), do: 0

  defp percentile(samples, q) do
    sorted = Enum.sort(samples)
    n = length(sorted)
    Enum.at(sorted, min(trunc((n - 1) * q + 0.5), n - 1))
  end

  defp round2(value), do: Float.round(value * 1.0, 2)
end
