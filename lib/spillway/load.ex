# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.Load do
  @moduledoc """
  How many HTTP users and WebSocket sessions this node has, and how many
  more it can take, for whatever is in front of it (Canalis): reported in
  the registration ping and on every response (`x-aqueduct-http-users`,
  `x-aqueduct-http-capacity`, `x-aqueduct-ws-sessions`,
  `x-aqueduct-ws-capacity`). See REGISTRATION.md.

  Both kinds of traffic spend the same CPU, so they share one budget:
  whatever one isn't using, the other can have. Every
  5 seconds this node reads how busy its schedulers were, how many
  jobs it accepted and how many WebSocket messages it moved. It splits the
  CPU in use between the two kinds by what each costs (from benchmark.md:
  about 700 jobs/s and 900 WebSocket round trips/s per CPU, which is
  1,800 messages: a client message and a backend one), and works out
  what one more user or session costs from that:

      headroom      = 80% - CPU in use
      http_capacity = users + headroom / CPU per user
      ws_capacity   = least of: sessions + headroom / CPU per session,
                      sessions + free memory / 50 KB,
                      SPILLWAY_WS_MAX_CLIENT_CONNECTIONS

  Neither is ever below what the node already has. With nothing to measure
  yet, a user is taken to send 10 jobs/s and a session 1 message/s.
  Aquifer reports the same four numbers the same way, with its own costs.
  """

  use GenServer
  require Logger

  @sample_ms 5_000
  @target_cpu 0.8
  # Weight of the newest reading in the running averages: about 30 seconds
  # of memory at one reading every 5 seconds.
  @smoothing 0.3

  # Measured per CPU: benchmark.md section 8 (jobs end to end, webhook
  # included) and Aquifer's benchmark.md section 13 (WebSocket round trips).
  @jobs_per_cpu 700
  # A round trip is two messages, a client one and a backend one, and
  # ws_message/0 counts each.
  @ws_messages_per_cpu 1_800
  @session_memory_kb 50

  # Assumed while there's no traffic to measure.
  @assumed_jobs_per_user 10
  @assumed_messages_per_session 1

  @max_http_capacity 10_000

  # Counter slots.
  @jobs 1
  @ws_messages 2

  # Result slots; @ready is 0 until the first reading.
  @http_users 1
  @http_capacity 2
  @ws_sessions 3
  @ws_capacity 4
  @ready 5

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Counts an accepted job (or webhook delivery)."
  def job_accepted, do: count(@jobs)

  @doc "Counts a WebSocket message, either direction."
  def ws_message, do: count(@ws_messages)

  @doc """
  The last reading: `%{http_users:, http_capacity:, ws_sessions:,
  ws_capacity:}`, or nil before the first one.
  """
  def current do
    case :persistent_term.get({__MODULE__, :result}, nil) do
      nil ->
        nil

      ref ->
        if :atomics.get(ref, @ready) == 1 do
          %{
            http_users: :atomics.get(ref, @http_users),
            http_capacity: :atomics.get(ref, @http_capacity),
            ws_sessions: :atomics.get(ref, @ws_sessions),
            ws_capacity: :atomics.get(ref, @ws_capacity)
          }
        end
    end
  end

  @doc """
  The capacities for one set of readings. `cpu` is the share of the node's
  CPU in use (0-1), `job_rate` and `message_rate` per second.
  """
  def compute(%{} = r) do
    cpus = max(r.cpus, 1)
    http_need = r.job_rate / (@jobs_per_cpu * cpus)
    ws_need = r.message_rate / (@ws_messages_per_cpu * cpus)
    need = http_need + ws_need

    {http_cpu, ws_cpu} =
      if need > 0, do: {r.cpu * http_need / need, r.cpu * ws_need / need}, else: {0.0, 0.0}

    headroom = max(@target_cpu - r.cpu, 0.0)

    per_user = cost(http_cpu, r.http_users, @assumed_jobs_per_user / (@jobs_per_cpu * cpus))

    per_session =
      cost(ws_cpu, r.ws_sessions, @assumed_messages_per_session / (@ws_messages_per_cpu * cpus))

    http_capacity = min(r.http_users + floor(headroom / per_user), @max_http_capacity)

    by_memory =
      if r.memory_limit_mb > 0,
        do:
          r.ws_sessions +
            floor(max(r.memory_limit_mb - r.memory_mb, 0) * 1024 / @session_memory_kb),
        else: r.max_sessions

    ws_capacity =
      Enum.min([r.ws_sessions + floor(headroom / per_session), by_memory, r.max_sessions])

    %{
      http_users: r.http_users,
      http_capacity: max(http_capacity, r.http_users),
      ws_sessions: r.ws_sessions,
      ws_capacity: max(ws_capacity, r.ws_sessions)
    }
  end

  # CPU one user or session costs: measured when there are some and they
  # used any, assumed otherwise.
  defp cost(cpu, count, _assumed) when count > 0 and cpu > 0, do: cpu / count
  defp cost(_cpu, _count, assumed), do: assumed

  @impl true
  def init(_opts) do
    ensure_counters()
    result = :atomics.new(5, signed: true)
    :persistent_term.put({__MODULE__, :result}, result)
    # Scheduler busy time is only kept while this flag is on, and only for
    # as long as the process that turned it on lives.
    :erlang.system_flag(:scheduler_wall_time, true)
    # The first reading waits a second, so it averages over a real window
    # rather than the microseconds since init.
    Process.send_after(self(), :sample, 1_000)

    {:ok,
     %{result: result, scheduler: :scheduler.sample(), counts: counts(), at: now(), averages: nil}}
  end

  @impl true
  def handle_info(:sample, state) do
    Process.send_after(self(), :sample, @sample_ms)
    {:noreply, sample(state)}
  end

  defp sample(state) do
    scheduler = :scheduler.sample()
    {jobs, messages} = counts()
    at = now()
    seconds = max(at - state.at, 1) / 1_000
    {last_jobs, last_messages} = state.counts

    reading = %{
      cpu: busy(state.scheduler, scheduler),
      job_rate: (jobs - last_jobs) / seconds,
      message_rate: (messages - last_messages) / seconds
    }

    averages = smooth(state.averages, reading)
    {sessions, max_sessions} = sessions()

    load =
      averages
      |> Map.merge(%{
        cpus: System.schedulers_online(),
        http_users: Spillway.UserLoad.active_users(),
        ws_sessions: sessions,
        max_sessions: max_sessions,
        memory_limit_mb: Spillway.Admission.memory_limit_mb(),
        memory_mb: Spillway.Admission.measured_memory_mb()
      })
      |> compute()

    publish(state.result, load)
    %{state | scheduler: scheduler, counts: {jobs, messages}, at: at, averages: averages}
  rescue
    err ->
      Logger.warning("load: reading failed: #{Exception.message(err)}")
      state
  end

  defp smooth(nil, reading), do: reading

  defp smooth(previous, reading),
    do: Map.new(reading, fn {k, v} -> {k, previous[k] + @smoothing * (v - previous[k])} end)

  # Share of the normal schedulers' time spent busy between two samples.
  defp busy(from, to) do
    :scheduler.utilization(from, to)
    |> Enum.flat_map(fn
      {:normal, _id, share, _} -> [share]
      _ -> []
    end)
    |> case do
      [] -> 0.0
      shares -> Enum.sum(shares) / length(shares)
    end
  end

  # Open sessions and the most this node takes. No queue, no sessions.
  defp sessions do
    %{clients: clients, max_client_connections: max} = Spillway.WebSocketQueue.snapshot()
    {clients, max}
  catch
    :exit, _ -> {0, 0}
  end

  defp publish(ref, load) do
    :atomics.put(ref, @http_users, load.http_users)
    :atomics.put(ref, @http_capacity, load.http_capacity)
    :atomics.put(ref, @ws_sessions, load.ws_sessions)
    :atomics.put(ref, @ws_capacity, load.ws_capacity)
    :atomics.put(ref, @ready, 1)
  end

  defp count(slot) do
    case :persistent_term.get({__MODULE__, :counters}, nil) do
      nil -> :ok
      ref -> :counters.add(ref, slot, 1)
    end
  end

  defp counts do
    ref = :persistent_term.get({__MODULE__, :counters})
    {:counters.get(ref, @jobs), :counters.get(ref, @ws_messages)}
  end

  defp ensure_counters do
    if :persistent_term.get({__MODULE__, :counters}, nil) == nil do
      :persistent_term.put({__MODULE__, :counters}, :counters.new(2, [:write_concurrency]))
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
