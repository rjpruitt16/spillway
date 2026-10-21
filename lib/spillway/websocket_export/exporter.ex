# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketExport.Exporter do
  @moduledoc """
  Sends the WebSocket export queue to SPILLWAY_WS_EXPORT_URL in batches and
  deletes each batch only after a 2xx (see Spillway.WebSocketExport).

  - Batches hold up to SPILLWAY_WS_EXPORT_BATCH messages and
    SPILLWAY_WS_EXPORT_MAX_BATCH_BYTES encoded; what doesn't fit leads the
    next batch.
  - A 413 halves the batch and retries at once; the size doubles back
    after 10 successful batches in a row. A single message still refused
    with 413 is logged and skipped so it can't block the queue.
  - Other failures retry with backoff up to 30 s.
  - The local queue is capped at SPILLWAY_WS_EXPORT_MAX_PENDING (oldest
    dropped, logged); the Valkey stream is capped by MAXLEN.
  """

  use GenServer
  require Logger

  alias Spillway.WebSocketExport
  alias Spillway.WebSocketExport.{LocalQueue, ValkeyQueue}
  alias Spillway.WebSocketStore.Valkey

  @grow_after 10
  @max_backoff_ms 30_000
  @idle_poll_ms 250
  @sweep_every_ms 600_000
  @redix_name :spillway_ws_export_valkey

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  @doc "The supervisor child, or nothing when export is off."
  def child_specs do
    config = WebSocketExport.load_config()
    if config.enabled, do: [{__MODULE__, config}], else: []
  end

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)

    with {:ok, backend} <- open_backend() do
      state = %{
        config: config,
        backend: backend,
        consumer: Spillway.Redirect.self_machine_id(),
        limit: config.batch,
        successes: 0,
        backoff_ms: 1_000,
        last_sweep: 0
      }

      # Only now: without the queue (and Valkey's group), appends would fill
      # the queue with nothing reading it.
      WebSocketExport.set_enabled(%{max_pending: config.max_pending})
      Logger.info("websocket export: sending transcripts to #{config.url} as #{state.consumer}")
      send(self(), :export)
      {:ok, state}
    else
      {:error, reason} ->
        Logger.warning("websocket export: disabled: #{inspect(reason)}")
        :ignore
    end
  end

  defp open_backend do
    if Valkey.enabled?() do
      with {:ok, conn} <- Redix.start_link(Valkey.url(), name: @redix_name, sync_connect: true),
           :ok <- ValkeyQueue.ensure_group(conn) do
        {:ok, {:valkey, conn}}
      end
    else
      LocalQueue.init!()
      {:ok, :local}
    end
  end

  @impl true
  def handle_info(:export, state) do
    state = maybe_maintain(state)

    case export_once(state) do
      {:sent, 0, state} ->
        Process.send_after(
          self(),
          :export,
          if(state.backend == :local, do: @idle_poll_ms, else: 0)
        )

        {:noreply, state}

      {:sent, _n, state} ->
        send(self(), :export)
        {:noreply, state}

      {:retry_now, state} ->
        send(self(), :export)
        {:noreply, state}

      {:error, reason, state} ->
        Logger.warning("websocket export: #{reason}; retrying in #{state.backoff_ms} ms")
        Process.send_after(self(), :export, state.backoff_ms)
        {:noreply, %{state | backoff_ms: min(state.backoff_ms * 2, @max_backoff_ms)}}
    end
  end

  def handle_info({:EXIT, _pid, reason}, state), do: {:stop, reason, state}
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{backend: {:valkey, conn}} = state) do
    ValkeyQueue.leave(conn, state.consumer)
  end

  def terminate(_reason, _state), do: :ok

  defp maybe_maintain(state) do
    now = System.monotonic_time(:millisecond)

    case state.backend do
      :local ->
        case LocalQueue.enforce_cap(state.config.max_pending) do
          0 ->
            :ok

          dropped ->
            Logger.warning(
              "websocket export: queue over SPILLWAY_WS_EXPORT_MAX_PENDING; dropped the oldest #{dropped}"
            )
        end

        state

      {:valkey, conn} when now - state.last_sweep > @sweep_every_ms ->
        ValkeyQueue.sweep(conn, state.consumer)
        %{state | last_sweep: now}

      _ ->
        state
    end
  end

  defp export_once(state) do
    case read(state) do
      {:ok, []} ->
        {:sent, 0, state}

      {:ok, entries} ->
        {events, ids} = fill_batch(entries, state.config)
        send_batch(events, ids, state)

      {:error, reason} ->
        {:error, "read export queue: #{inspect(reason)}", state}
    end
  end

  defp read(%{backend: :local, limit: limit}),
    do: {:ok, LocalQueue.read(limit) |> Enum.map(&local_entry/1)}

  defp read(%{backend: {:valkey, conn}} = state) do
    with {:ok, entries} <- ValkeyQueue.read(conn, state.consumer, state.limit, 1_000) do
      {:ok, Enum.map(entries, &valkey_entry/1)}
    end
  end

  defp local_entry({key, {session_id, stream_id, direction, envelope, recorded_at}}),
    do: {key, {session_id, stream_id, direction, envelope, recorded_at}}

  defp local_entry({key, _other}), do: {key, nil}

  defp valkey_entry({id, fields}) do
    with session_id when is_binary(session_id) <- fields["session_id"],
         {:ok, envelope} <- Jason.decode(fields["envelope"] || ""),
         {recorded_at, ""} <- Integer.parse(fields["recorded_at"] || "") do
      {id, {session_id, fields["stream_id"], fields["direction"], envelope, recorded_at}}
    else
      _ -> {id, nil}
    end
  end

  # Fills the batch up to max_batch_bytes. Entries that don't decode are
  # acknowledged with it and dropped.
  defp fill_batch(entries, config) do
    {events, ids, _size} =
      Enum.reduce_while(entries, {[], [], 0}, fn
        {id, nil}, {events, ids, size} ->
          {:cont, {events, [id | ids], size}}

        {id, {session_id, stream_id, direction, envelope, recorded_at}}, {events, ids, size} ->
          event =
            WebSocketExport.event(
              session_id,
              stream_id,
              direction,
              envelope,
              recorded_at,
              config.payloads
            )

          bytes = byte_size(Jason.encode!(event))

          if events != [] and size + bytes > config.max_batch_bytes,
            do: {:halt, {events, ids, size}},
            else: {:cont, {[event | events], [id | ids], size + bytes}}
      end)

    {Enum.reverse(events), Enum.reverse(ids)}
  end

  defp send_batch([], ids, state), do: acknowledge(ids, 0, state)

  defp send_batch(events, ids, state) do
    case post(events, state) do
      :ok ->
        acknowledge(ids, length(events), state)

      :too_large when length(events) > 1 ->
        {:retry_now, %{state | limit: max(1, div(length(events), 2)), successes: 0}}

      :too_large ->
        [event] = events

        Logger.warning(
          "websocket export: receiver refused one message as too large (session #{event["session_id"]}, stream #{event["stream_id"]}); skipping it"
        )

        acknowledge(ids, 0, state)

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp acknowledge(ids, sent, state) do
    result =
      case state.backend do
        :local -> LocalQueue.ack(ids)
        {:valkey, conn} -> ValkeyQueue.ack(conn, ids)
      end

    case result do
      :ok -> {:sent, sent, grow(%{state | backoff_ms: 1_000})}
      {:error, reason} -> {:error, "acknowledge exported batch: #{inspect(reason)}", state}
    end
  end

  defp grow(%{limit: limit, config: %{batch: batch}} = state) when limit >= batch, do: state

  defp grow(state) do
    successes = state.successes + 1

    if successes >= @grow_after,
      do: %{state | limit: min(state.config.batch, state.limit * 2), successes: 0},
      else: %{state | successes: successes}
  end

  defp post(events, state) do
    body =
      Jason.encode!(%{schema: WebSocketExport.schema(), node_id: state.consumer, events: events})

    headers =
      [{"content-type", "application/json"}] ++
        if(state.config.token, do: [{"authorization", "Bearer " <> state.config.token}], else: [])

    # Finch doesn't follow redirects, so a 3xx can't send the token elsewhere;
    # it counts as a failed batch.
    case Finch.build(:post, state.config.url, headers, body)
         |> Finch.request(Spillway.Finch, receive_timeout: 10_000, request_timeout: 10_000) do
      {:ok, %Finch.Response{status: status}} when status in 200..299 ->
        :ok

      {:ok, %Finch.Response{status: 413}} ->
        :too_large

      {:ok, %Finch.Response{status: status}} ->
        {:error, "receiver answered #{status} to a batch of #{length(events)}"}

      {:error, reason} ->
        {:error, "send batch of #{length(events)}: #{inspect(reason)}"}
    end
  end
end
