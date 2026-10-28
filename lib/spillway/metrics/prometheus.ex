# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.Metrics.Prometheus do
  @moduledoc """
  The built-in metrics adapter behind GET /metrics, in Prometheus text
  format. Mirrors Aquifer's internal/metrics Prometheus adapter, with metric
  names under `spillway_` instead of `aquifer_`.

  Labels are by target API (scheme and host, or pool), never by tenant:
  tenant IDs are unbounded and often customer data. Targets are capped at
  1000: without SPILLWAY_ALLOWED_URL_DOMAINS, URL jobs can name any host, and
  every counter is kept for the life of the node. Past the cap, new targets
  share the label "other".

  Counters live in a public ETS table, so recording an event never waits on a
  process. This process owns the table and keeps the fairness samples, which
  only a scrape touches.
  """

  use GenServer

  @behaviour Spillway.Metrics

  @table :spillway_prometheus
  @max_targets 1000
  # How far back the fairness gauge compares service.
  @fairness_window_ms 60_000
  @buckets [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600]

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @doc "Every metric, in Prometheus text format."
  def render, do: GenServer.call(__MODULE__, :render, 15_000)

  # ---- Events ----

  # Queue depth and flow rate are read from the live queues at scrape time
  # instead: these events come from many queues under one target label, so
  # the last one to report would win.
  @impl Spillway.Metrics
  def job_queued(_user_id, _upstream), do: :ok
  @impl Spillway.Metrics
  def job_dispatched(_user_id, _upstream), do: :ok
  @impl Spillway.Metrics
  def job_completed(_user_id, _upstream, _duration_ms), do: :ok
  @impl Spillway.Metrics
  def job_failed(_user_id, _upstream, _reason), do: :ok
  # Registration pings and drain-mode ledger flushes, not completion webhooks.
  @impl Spillway.Metrics
  def webhook_delivered(_url, _attempt), do: :ok
  @impl Spillway.Metrics
  def webhook_failed(_url, _attempts), do: :ok
  @impl Spillway.Metrics
  def queue_depth(_upstream, _depth), do: :ok
  @impl Spillway.Metrics
  def flow_rate(_upstream, _rps), do: :ok
  @impl Spillway.Metrics
  def drain_flush_succeeded(_instance_key, _ledger_size), do: :ok
  @impl Spillway.Metrics
  def drain_flush_failed(_instance_key, _ledger_size), do: :ok

  @impl Spillway.Metrics
  def job_waited(_user_id, upstream, wait_ms), do: observe({:wait, target(upstream)}, wait_ms)

  @impl Spillway.Metrics
  def job_finished(_user_id, upstream, outcome, total_ms) do
    target = target(upstream)
    bump({:jobs, target, outcome})
    observe({:duration, target, outcome}, total_ms)
  end

  @impl Spillway.Metrics
  def dispatch_attempt(upstream, status),
    do: bump({:attempts, target(upstream), status_class(status)})

  @impl Spillway.Metrics
  def admission_rejected(reason), do: bump({:rejected, reason})

  @impl Spillway.Metrics
  def webhook_job_finished(outcome), do: bump({:webhooks, outcome})

  # ---- Recording ----

  defp bump(key) do
    :ets.update_counter(@table, key, {2, 1}, {key, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # A histogram is one row: {key, count, sum_us, bucket_1, ..., bucket_n},
  # buckets not cumulative.
  defp observe(key, ms) do
    seconds = ms / 1000
    index = Enum.find_index(@buckets, &(seconds <= &1))
    ops = [{2, 1}, {3, round(ms * 1000)}] ++ if(index, do: [{4 + index, 1}], else: [])
    default = List.to_tuple([key, 0, 0 | List.duplicate(0, length(@buckets))])
    :ets.update_counter(@table, key, ops, default)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # The target label for upstream: itself for the first @max_targets seen,
  # "other" after that.
  defp target(upstream) do
    key = {:target_seen, upstream}

    cond do
      :ets.member(@table, key) ->
        upstream

      :ets.update_counter(@table, :target_count, {2, 1}, {:target_count, 0}) <= @max_targets ->
        :ets.insert(@table, {key})
        upstream

      true ->
        :ets.update_counter(@table, :target_count, {2, -1})
        "other"
    end
  rescue
    ArgumentError -> upstream
  end

  # 429 on its own, since it's the backend saying "slow down" rather than
  # failing.
  defp status_class(0), do: "error"
  defp status_class(429), do: "429"
  defp status_class(status) when status in 100..599, do: "#{div(status, 100)}xx"
  defp status_class(_), do: "other"

  # ---- GenServer ----

  @impl GenServer
  def init(:ok) do
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    {:ok, %{samples: %{}}}
  end

  @impl GenServer
  def handle_call(:render, _from, state) do
    targets = target_stats()
    {fairness, samples} = fairness(targets, state.samples, System.monotonic_time(:millisecond))
    {:reply, IO.iodata_to_binary(write(targets, fairness)), %{state | samples: samples}}
  end

  # The live queues on this node, by target.
  defp target_stats do
    Spillway.Cluster.all_account_queues()
    |> Enum.uniq()
    |> Enum.filter(&(node(&1) == node()))
    |> Enum.flat_map(fn pid ->
      try do
        [Spillway.AccountQueue.snapshot(pid)]
      catch
        :exit, _reason -> []
      end
    end)
    |> Enum.group_by(&to_string(&1.upstream))
    |> Enum.map(fn {target, queues} ->
      active = Enum.filter(queues, & &1.active)

      %{
        target: target,
        active_queues: length(active),
        backlog: Enum.sum(Enum.map(queues, & &1.backlog)),
        flow_rate: Enum.sum(Enum.map(active, & &1.rps)),
        queues: queues
      }
    end)
    |> Enum.sort_by(& &1.target)
  end

  # Jain's index over the queues that have work waiting now, by how many
  # jobs each finished over about the last minute: (Σx)² / (n·Σx²), 1.0
  # when they were all served equally. Queues created within the window are
  # left out, since a newcomer's short history would look like unfairness.
  # Samples for targets with no queues left are dropped.
  defp fairness(targets, samples, now) do
    Enum.reduce(targets, {%{}, %{}}, fn t, {indexes, kept} ->
      current = {now, Map.new(t.queues, &{&1.queue_key, &1.served})}
      history = Map.get(samples, t.target, [])
      # Compare against the newest sample at least a window old (or the
      # oldest, early on), and drop the ones before it.
      from =
        history
        |> Enum.with_index()
        |> Enum.filter(fn {{at, _}, _i} -> now - at >= @fairness_window_ms end)
        |> List.last()
        |> case do
          nil -> if history == [], do: nil, else: 0
          {_, i} -> i
        end

      history = if from, do: Enum.drop(history, from), else: history
      kept = Map.put(kept, t.target, history ++ [current])

      case history do
        [{_at, then} | _] when t.queues != [] ->
          xs =
            for q <- t.queues,
                q.backlog > 0,
                Map.has_key?(then, q.queue_key),
                do: q.served - Map.fetch!(then, q.queue_key)

          sum = Enum.sum(xs)
          squares = Enum.sum(Enum.map(xs, &(&1 * &1)))

          if xs == [] or squares == 0,
            do: {indexes, kept},
            else: {Map.put(indexes, t.target, sum * sum / (length(xs) * squares)), kept}

        _ ->
          {indexes, kept}
      end
    end)
  end

  # ---- Rendering ----

  defp rows(match), do: :ets.match_object(@table, match) |> Enum.sort()

  defp write(targets, fairness) do
    [
      help(
        "spillway_node_info",
        "gauge",
        "This node's id, as sent to backends in X-Aqueduct-Node-Id."
      ),
      "spillway_node_info{node_id=#{label(Spillway.NodeId.get())}} 1\n",
      counters(
        "spillway_jobs_total",
        "Jobs that reached a terminal state, by outcome (completed, failed, expired).",
        {{:jobs, :_, :_}, :_},
        fn {:jobs, target, outcome} -> "target=#{label(target)},outcome=#{label(outcome)}" end
      ),
      counters(
        "spillway_dispatch_attempts_total",
        "Attempts at a target API, retries included, by result (2xx, 4xx, 429, 5xx, error = no response).",
        {{:attempts, :_, :_}, :_},
        fn {:attempts, target, class} ->
          "target=#{label(target)},status_class=#{label(class)}"
        end
      ),
      counters(
        "spillway_admission_rejected_total",
        "Jobs refused with 429, by reason.",
        {{:rejected, :_}, :_},
        fn {:rejected, reason} -> "reason=#{label(reason)}" end
      ),
      counters(
        "spillway_webhooks_total",
        "Completion webhooks that finished, by outcome (delivered, or failed after its retries).",
        {{:webhooks, :_}, :_},
        fn {:webhooks, outcome} -> "outcome=#{label(outcome)}" end
      ),
      help(
        "spillway_queue_wait_seconds",
        "histogram",
        "Time from submission to a job's first dispatch."
      ),
      Enum.map(histograms(:wait), fn row ->
        {:wait, target} = elem(row, 0)
        histogram("spillway_queue_wait_seconds", "target=#{label(target)}", row)
      end),
      help(
        "spillway_job_duration_seconds",
        "histogram",
        "Time from submission to a terminal state, by outcome."
      ),
      Enum.map(histograms(:duration), fn row ->
        {:duration, target, outcome} = elem(row, 0)

        histogram(
          "spillway_job_duration_seconds",
          "target=#{label(target)},outcome=#{label(outcome)}",
          row
        )
      end),
      gauges(
        "spillway_active_queues",
        "Queues on this node with work waiting for the target API (one per tenant with account queues on).",
        targets,
        & &1.active_queues
      ),
      gauges(
        "spillway_backlog_jobs",
        "Jobs on this node queued or in flight for the target API.",
        targets,
        & &1.backlog
      ),
      gauges(
        "spillway_flow_rate_rps",
        "Sum of the active queues' current dispatch rates, after the target API's pacing headers.",
        targets,
        & &1.flow_rate
      ),
      gauges(
        "spillway_fairness_jain",
        "Jain's index (0-1) over queues with work waiting, by jobs each finished in about the last minute. 1 is perfectly even.",
        Enum.filter(targets, &Map.has_key?(fairness, &1.target)),
        &fairness[&1.target]
      )
    ]
  end

  defp counters(name, text, match, labels) do
    [
      help(name, "counter", text),
      Enum.map(rows(match), fn {key, n} -> "#{name}{#{labels.(key)}} #{n}\n" end)
    ]
  end

  defp gauges(name, text, targets, value) do
    [
      help(name, "gauge", text),
      Enum.map(targets, fn t -> "#{name}{target=#{label(t.target)}} #{number(value.(t))}\n" end)
    ]
  end

  defp histograms(kind) do
    :ets.select(@table, [
      {:"$1",
       [{:==, {:element, 1, {:element, 1, :"$1"}}, kind}, {:is_tuple, {:element, 1, :"$1"}}],
       [:"$1"]}
    ])
    |> Enum.sort()
  rescue
    ArgumentError -> []
  end

  defp histogram(name, labels, row) do
    [_key, count, sum_us | buckets] = Tuple.to_list(row)

    {lines, _} =
      @buckets
      |> Enum.zip(buckets)
      |> Enum.map_reduce(0, fn {le, n}, acc ->
        {"#{name}_bucket{#{labels},le=\"#{number(le)}\"} #{acc + n}\n", acc + n}
      end)

    [
      lines,
      "#{name}_bucket{#{labels},le=\"+Inf\"} #{count}\n",
      "#{name}_sum{#{labels}} #{number(sum_us / 1_000_000)}\n",
      "#{name}_count{#{labels}} #{count}\n"
    ]
  end

  defp help(name, kind, text), do: "# HELP #{name} #{text}\n# TYPE #{name} #{kind}\n"

  # A Prometheus label value: quoted, with \\, " and newlines escaped.
  defp label(value) do
    escaped =
      value
      |> to_string()
      |> String.replace("\\", "\\\\")
      |> String.replace("\"", "\\\"")
      |> String.replace("\n", "\\n")

    "\"" <> escaped <> "\""
  end

  defp number(value) when is_integer(value), do: Integer.to_string(value)

  defp number(value) when is_float(value) do
    rounded = Float.round(value, 6)

    if rounded == trunc(rounded),
      do: Integer.to_string(trunc(rounded)),
      else: Float.to_string(rounded)
  end
end
