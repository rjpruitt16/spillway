# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule SpillwayWeb.JobController do
  use SpillwayWeb, :controller

  alias Spillway.Job
  alias Spillway.IdempotentStore
  alias Spillway.AccountQueueRegistry
  alias Spillway.Admission
  alias Spillway.Proxy
  alias Spillway.Redirect
  alias SpillwayWeb.JobStreamController

  plug SpillwayWeb.NodeDraining when action in [:create, :proxy]
  plug :put_inbound_rps when action in [:create, :proxy]

  # The caller of a direct /proxy gets one rate: the lower of Spillway's and
  # the target's own X-Aqueduct-Rps (X-Aquifer-Rps as a fallback).
  defp upstream_rps(headers) do
    found = fn name ->
      Enum.find_value(headers, fn {k, v} -> String.downcase(k) == name && v end)
    end

    found.("x-aqueduct-rps") || found.("x-aquifer-rps")
  end

  @doc false
  def lower_rps(a, b) do
    case {positive(a), positive(b)} do
      {{:ok, av}, {:ok, bv}} -> if bv < av, do: b, else: a
      {{:ok, _}, _} -> a
      {_, {:ok, _}} -> b
      _ -> nil
    end
  end

  defp positive(value) when is_binary(value) do
    case Float.parse(value) do
      {n, ""} when n > 0 -> {:ok, n}
      _ -> :error
    end
  end

  defp positive(_value), do: :error

  defp put_rps(conn, nil), do: conn

  defp put_rps(conn, rps) do
    conn
    |> put_resp_header("x-aqueduct-rps", rps)
    |> put_resp_header("x-aquifer-rps", rps)
  end

  # Advertises this caller's share of Spillway's own inbound capacity, so a
  # caller that honors X-Aqueduct-Rps (Canalis, another Spillway or Aquifer)
  # paces itself. Set before anything else, so 429s carry it too.
  defp put_inbound_rps(conn, _opts) do
    case Spillway.InboundRate.advertised_rps(conn.params["user_id"]) do
      rps when is_number(rps) and rps > 0 ->
        # Two decimals would round a tiny positive share (many callers at a
        # low rate) to 0.00, which reads as no limit at all.
        value =
          if rps < 0.01,
            do: :erlang.float_to_binary(rps * 1.0, [:short]),
            else: :erlang.float_to_binary(rps * 1.0, decimals: 2)

        put_rps(conn, value)

      _ ->
        conn
    end
  end

  def create(conn, params) do
    case Job.new(with_max_retries_header(conn, params)) do
      {:error, reason} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: reason})

      {:ok, job} ->
        case validate_schema(job) do
          :ok -> submit(conn, job)
          {:error, mismatch} -> schema_mismatch(conn, mismatch)
        end
    end
  end

  defp submit(conn, job) do
    case AccountQueueRegistry.submit(job, account_queue_header(conn)) do
      {:duplicate, existing_id} ->
        conn
        |> put_status(:ok)
        |> json(duplicate_response(existing_id))

      {:accepted, accepted_job} ->
        conn
        |> put_queue_headers(accepted_job)
        |> put_status(:created)
        |> json(%{job_id: accepted_job.id, status: "queued"})

      {:rejected, reason, limit, current} ->
        conn
        |> put_queue_headers(job)
        |> admission_rejected(reason, limit, current)
    end
  end

  # Pool-routed jobs have no caller-supplied URL to check.
  defp validate_schema(%Job{url: url} = job) when is_binary(url),
    do: Spillway.L8.Schemas.validate(url, job.method, job.body)

  defp validate_schema(_job), do: :ok

  defp schema_mismatch(conn, %{route: route, schema_hash: hash, detail: detail}) do
    conn
    |> put_status(:unprocessable_entity)
    |> json(%{
      error: "request body does not match the schema the upstream advertises for #{route}",
      schema_route: route,
      schema_hash: hash,
      schema_errors: detail
    })
  end

  @doc """
  Proxy mode: try the upstream directly and synchronously first; fall back
  to the same durable-queue-and-SSE path create/2 + stream/2 always use,
  on this same connection, only on failure/overload/an already-open
  circuit breaker. See Spillway.Proxy for the actual decision
  logic -- this action is HTTP glue only.
  """
  def proxy(conn, params) do
    case Proxy.attempt_direct(with_max_retries_header(conn, params), account_queue_header(conn)) do
      {:error, reason} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: reason})

      {:admission_rejected, reason, limit, current} ->
        admission_rejected(conn, reason, limit, current)

      {:schema_mismatch, mismatch} ->
        schema_mismatch(conn, mismatch)

      {:duplicate, existing_job} ->
        stream_or_status_for_duplicate(conn, existing_job)

      {:direct, _job, response} ->
        ours = conn |> get_resp_header("x-aqueduct-rps") |> List.first()

        conn =
          response.headers
          |> Enum.reject(fn {k, _v} ->
            String.downcase(k) in ["x-aqueduct-rps", "x-aquifer-rps"]
          end)
          |> Enum.reduce(conn, fn {k, v}, c -> put_resp_header(c, k, v) end)
          |> put_rps(lower_rps(ours, upstream_rps(response.headers)))

        send_resp(conn, response.status, response.body)

      {:fallback, job, reason} ->
        # Subscribe before queueing: dispatch can start the moment the job
        # is queued, and a stream that subscribed afterwards would miss the
        # live "dispatching" event.
        Phoenix.PubSub.subscribe(Spillway.PubSub, "job:#{job.id}")

        case AccountQueueRegistry.enqueue_prepared(job, account_queue_header(conn)) do
          :ok ->
            conn
            |> put_queue_headers(job)
            |> JobStreamController.stream_events(job, reason, subscribed: true)

          {:rejected, limit_reason, limit, current} ->
            Phoenix.PubSub.unsubscribe(Spillway.PubSub, "job:#{job.id}")
            IdempotentStore.discard_refused(job)

            conn
            |> put_queue_headers(job)
            |> admission_rejected(limit_reason, limit, current)
        end

      {:redirected, {:direct, status, headers, body, region}} ->
        conn =
          Enum.reduce(headers, conn, fn {k, v}, c -> put_resp_header(c, k, v) end)
          |> put_resp_header("x-aquifer-served-by-region", region)

        send_resp(conn, status, body)

      {:redirected, {:relay, request_id, region}} ->
        JobStreamController.relay_stream(conn, request_id, region)

      {:redirect_exhausted, job_id} ->
        Spillway.Metrics.admission_rejected("redirect_exhausted")
        retry_after = Redirect.exhausted_retry_after_seconds()

        conn
        |> put_resp_header("retry-after", to_string(retry_after))
        |> put_status(429)
        |> json(%{
          error:
            "job #{job_id}: cross-region redirect exhausted, no known-live region could help",
          limit_reason: "redirect_exhausted"
        })
    end
  end

  # A duplicate of an already-terminal job has no cached response body to
  # replay (Job never persists it past the transient SSE/webhook payload)
  # -- opening a stream for it would just keepalive forever, since its
  # completed/failed event already fired before this request ever
  # subscribed. Return its current status synchronously instead; only a
  # still-in-flight duplicate gets a real stream. existing_job's own
  # :status field is frozen at construction time (always :queued) --
  # IdempotentStore.get_status/1 is the actual current status.
  defp stream_or_status_for_duplicate(conn, job) do
    case IdempotentStore.get_status(job.id) do
      status when status in ["completed", "failed"] ->
        json(conn, duplicate_response(job.id, status))

      _ ->
        JobStreamController.stream_events(conn, job)
    end
  end

  def show(conn, %{"id" => job_id}) do
    case IdempotentStore.get_job(job_id) do
      nil ->
        conn
        |> put_status(:not_found)
        |> json(%{error: "job not found"})

      job ->
        status = IdempotentStore.get_status(job_id)
        result = IdempotentStore.get_result(job_id)

        response =
          %{
            job_id: job_id,
            status: status,
            url: job.url,
            pool_id: job.pool_id,
            method: job.method,
            created_at: job.created_at,
            execute_before: job.execute_before
          }
          |> maybe_put_result(result)

        json(conn, response)
    end
  end

  defp duplicate_response(job_id, status \\ nil) do
    status = status || IdempotentStore.get_status(job_id) || "queued"
    result = IdempotentStore.get_result(job_id)

    %{job_id: job_id, status: status, duplicate: true}
    |> maybe_put_result(result)
  end

  defp maybe_put_result(payload, nil), do: payload
  defp maybe_put_result(payload, result), do: Map.put(payload, :result, result)

  defp admission_rejected(conn, reason, limit, current) do
    Spillway.Metrics.admission_rejected(reason)
    retry_after = Admission.retry_after_seconds()

    conn
    |> put_resp_header("retry-after", to_string(retry_after))
    |> put_status(429)
    |> json(%{
      error: "admission rejected: #{reason} (current #{current}, limit #{limit})",
      limit_reason: reason,
      limit: limit,
      current: current
    })
  end

  defp put_queue_headers(conn, job) do
    snapshot = AccountQueueRegistry.queue_snapshot(job)
    pressure = :erlang.float_to_binary(snapshot.admission_pressure * 1.0, decimals: 3)

    conn
    |> put_resp_header("x-aqueduct-active-queues", to_string(snapshot.active_queues))
    |> put_resp_header("x-aqueduct-upstream-backlog", to_string(snapshot.upstream_backlog))
    |> put_resp_header("x-aqueduct-queue-backlog", to_string(snapshot.queue_backlog))
    |> put_resp_header("x-aqueduct-admission-pressure", pressure)
    |> put_resp_header("x-ezthrottle-active-queues", to_string(snapshot.active_queues))
    |> put_resp_header("x-ezthrottle-upstream-backlog", to_string(snapshot.upstream_backlog))
    |> put_resp_header("x-ezthrottle-queue-backlog", to_string(snapshot.queue_backlog))
    |> put_resp_header("x-ezthrottle-admission-pressure", pressure)
  end

  # Reads X-Aqueduct-Account-Queue first, falling back to
  # X-EZThrottle-Account-Queue — this is the client-facing request-header
  # path for opting a job into per-tenant isolation directly. It previously
  # didn't exist at all: account-queue mode could only be toggled by the
  # *upstream's response* headers or static config, with no way for the
  # client submitting the job to ask for isolation up front.
  # X-Aqueduct-Max-Retries (or X-EZThrottle-Max-Retries) overrides the body's
  # max_retries, and X-Aqueduct-Webhook-Max-Retries the body's
  # webhook_max_retries; Job.new/1 validates both.
  defp with_max_retries_header(conn, params) do
    params
    |> put_header_param(conn, "max-retries", "max_retries")
    |> put_header_param(conn, "webhook-max-retries", "webhook_max_retries")
  end

  defp put_header_param(params, conn, header, param) do
    case Plug.Conn.get_req_header(conn, "x-aqueduct-" <> header) ++
           Plug.Conn.get_req_header(conn, "x-ezthrottle-" <> header) do
      [value | _] -> Map.put(params, param, value)
      [] -> params
    end
  end

  defp account_queue_header(conn) do
    case Plug.Conn.get_req_header(conn, "x-aqueduct-account-queue") do
      [val | _] ->
        val

      [] ->
        case Plug.Conn.get_req_header(conn, "x-ezthrottle-account-queue") do
          [val | _] -> val
          [] -> nil
        end
    end
  end
end
