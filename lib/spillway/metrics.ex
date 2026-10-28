# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.Metrics do
  @moduledoc """
  Pluggable metrics adapter entrypoint.

  Configure `:metrics_adapter` to route lifecycle events to a custom module.
  """

  @type user_id :: String.t()
  @type upstream :: String.t()
  @type url :: String.t()

  @callback job_queued(user_id(), upstream()) :: :ok
  @callback job_dispatched(user_id(), upstream()) :: :ok
  @callback job_completed(user_id(), upstream(), duration_ms :: integer()) :: :ok
  @callback job_failed(user_id(), upstream(), reason :: String.t()) :: :ok
  @callback webhook_delivered(url(), attempt :: integer()) :: :ok
  @callback webhook_failed(url(), attempts :: integer()) :: :ok
  @callback queue_depth(upstream(), depth :: integer()) :: :ok
  @callback flow_rate(upstream(), rps :: float()) :: :ok
  # Only ever fire when drain mode is enabled (see Spillway.DrainFlush) --
  # unreached on a deployment that never turns it on.
  @callback drain_flush_succeeded(instance_key :: String.t(), ledger_size :: integer()) :: :ok
  @callback drain_flush_failed(instance_key :: String.t(), ledger_size :: integer()) :: :ok

  # Optional, for adapters that want job timings, attempts and refusals (the
  # built-in Spillway.Metrics.Prometheus behind GET /metrics). Mirrors
  # Aquifer's JobTimingMetrics, DispatchMetrics, AdmissionMetrics and
  # WebhookJobMetrics. Jobs are counted from job_finished, which leaves out
  # Spillway's internal completion-webhook jobs; those come from
  # webhook_job_finished.
  @callback job_waited(user_id(), upstream(), wait_ms :: integer()) :: :ok
  @callback job_finished(user_id(), upstream(), outcome :: String.t(), total_ms :: integer()) ::
              :ok
  @callback dispatch_attempt(upstream(), status :: integer()) :: :ok
  @callback admission_rejected(reason :: String.t()) :: :ok
  @callback webhook_job_finished(outcome :: String.t()) :: :ok
  @optional_callbacks job_waited: 3,
                      job_finished: 4,
                      dispatch_attempt: 2,
                      admission_rejected: 1,
                      webhook_job_finished: 1

  def job_queued(user_id, upstream), do: call(:job_queued, [user_id, upstream])
  def job_dispatched(user_id, upstream), do: call(:job_dispatched, [user_id, upstream])

  def job_completed(user_id, upstream, duration_ms),
    do: call(:job_completed, [user_id, upstream, duration_ms])

  def job_failed(user_id, upstream, reason), do: call(:job_failed, [user_id, upstream, reason])
  def webhook_delivered(url, attempt), do: call(:webhook_delivered, [url, attempt])
  def webhook_failed(url, attempts), do: call(:webhook_failed, [url, attempts])
  def queue_depth(upstream, depth), do: call(:queue_depth, [upstream, depth])
  def flow_rate(upstream, rps), do: call(:flow_rate, [upstream, rps])

  def drain_flush_succeeded(instance_key, ledger_size),
    do: call(:drain_flush_succeeded, [instance_key, ledger_size])

  def drain_flush_failed(instance_key, ledger_size),
    do: call(:drain_flush_failed, [instance_key, ledger_size])

  @doc "A job's first dispatch: how long it waited since it was submitted."
  def observe_job_waited(%Spillway.Job{} = job, upstream) do
    if Spillway.Job.attempts(job) == 0 and not Spillway.Job.webhook_delivery_job?(job),
      do: call_optional(:job_waited, [job.user_id, upstream, since_created(job)])

    :ok
  end

  @doc """
  A job reached a terminal state (completed, failed or expired). status is
  the target's final response status, 0 if none. A completion webhook is
  delivered only on a 2xx: a receiver's 401 is one it turned down.
  """
  def observe_job_finished(%Spillway.Job{} = job, upstream, outcome, status) do
    if Spillway.Job.webhook_delivery_job?(job) do
      delivered = outcome == "completed" and is_integer(status) and status in 200..299
      call_optional(:webhook_job_finished, [if(delivered, do: "delivered", else: "failed")])
    else
      call_optional(:job_finished, [job.user_id, upstream, outcome, since_created(job)])
    end

    :ok
  end

  @doc "One attempt at a target API, retries included. status is 0 when no response came back."
  def observe_dispatch(%Spillway.Job{} = job, upstream, status) do
    if not Spillway.Job.webhook_delivery_job?(job),
      do: call_optional(:dispatch_attempt, [upstream, status || 0])

    :ok
  end

  def admission_rejected(reason), do: call_optional(:admission_rejected, [to_string(reason)])

  def adapter, do: Application.get_env(:spillway, :metrics_adapter, Spillway.Metrics.Noop)

  defp since_created(%Spillway.Job{created_at: created_at}) when is_integer(created_at),
    do: max(System.system_time(:millisecond) - created_at, 0)

  defp since_created(_job), do: 0

  defp call_optional(event, args) do
    adapter = adapter()

    if Code.ensure_loaded?(adapter) and function_exported?(adapter, event, length(args)),
      do: apply(adapter, event, args)

    :ok
  rescue
    _ -> :ok
  end

  def upstream(raw_url) do
    case URI.parse(raw_url) do
      %URI{scheme: scheme, host: host} when is_binary(scheme) and is_binary(host) ->
        "#{scheme}://#{host}"

      _ ->
        raw_url
    end
  end

  defp call(event, args) do
    adapter =
      Application.get_env(:spillway, :metrics_adapter, Spillway.Metrics.Noop)

    apply(adapter, event, args)
  rescue
    _ -> :ok
  end
end
