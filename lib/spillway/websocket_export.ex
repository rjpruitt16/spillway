# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketExport do
  @moduledoc """
  WebSocket transcript export, the same as Aquifer's (AQUIFER_WS_EXPORT_*).

  Transcripts stay a replay window (SPILLWAY_WS_STREAM_TTL_SECONDS). Export
  copies every recorded message to a receiver you run, such as Inlet, which
  can keep it for as long as you like, for example in a Kafka topic.

  Each append also queues an export copy in the same atomic write as the
  transcript entry: a RocksDB batch with the local store, or one Lua script
  with Valkey. Spillway.WebSocketExport.Exporter POSTs the queue in batches
  (`aquifer.websocket_batch.v1`) to SPILLWAY_WS_EXPORT_URL and deletes each
  batch only after a 2xx. Delivery is at least once: receivers drop repeats
  by session_id + stream_id, and order a session by stream_id.

  Off by default (SPILLWAY_WS_EXPORT_ENABLED).
  """

  require Logger

  defstruct enabled: false,
            url: nil,
            token: nil,
            batch: 500,
            max_batch_bytes: 4 * 1024 * 1024,
            max_pending: 1_000_000,
            payloads: true

  @schema "aquifer.websocket_batch.v1"
  def schema, do: @schema

  @doc """
  Reads SPILLWAY_WS_EXPORT_*. Off unless SPILLWAY_WS_EXPORT_ENABLED=true, and
  off with a warning without a URL, or with an http URL that isn't loopback
  (unless SPILLWAY_WS_EXPORT_ALLOW_INSECURE=true).
  """
  def load_config do
    config = %__MODULE__{
      enabled: env_bool("SPILLWAY_WS_EXPORT_ENABLED", false),
      url: blank_to_nil(System.get_env("SPILLWAY_WS_EXPORT_URL")),
      token: blank_to_nil(System.get_env("SPILLWAY_WS_EXPORT_TOKEN")),
      batch: positive_integer("SPILLWAY_WS_EXPORT_BATCH", 500),
      max_batch_bytes: positive_integer("SPILLWAY_WS_EXPORT_MAX_BATCH_BYTES", 4 * 1024 * 1024),
      max_pending: positive_integer("SPILLWAY_WS_EXPORT_MAX_PENDING", 1_000_000),
      payloads: env_bool("SPILLWAY_WS_EXPORT_PAYLOADS", true)
    }

    cond do
      not config.enabled ->
        config

      config.url == nil ->
        Logger.warning("websocket export: SPILLWAY_WS_EXPORT_URL is empty; export stays off")
        %{config | enabled: false}

      true ->
        case check_url(config.url, env_bool("SPILLWAY_WS_EXPORT_ALLOW_INSECURE", false)) do
          :ok ->
            config

          {:error, reason} ->
            Logger.warning("websocket export: #{reason}; export stays off")
            %{config | enabled: false}
        end
    end
  end

  @doc """
  https only, so the bearer token and transcripts aren't sent in cleartext;
  http for loopback, or anywhere when allow_insecure (a private network you
  trust).
  """
  def check_url(url, allow_insecure) do
    case URI.parse(url) do
      %URI{scheme: "https", host: host} when is_binary(host) and host != "" ->
        :ok

      %URI{scheme: "http", host: host} when is_binary(host) and host != "" ->
        if allow_insecure or loopback?(host),
          do: :ok,
          else:
            {:error,
             "SPILLWAY_WS_EXPORT_URL must use https (http is allowed for loopback, or with SPILLWAY_WS_EXPORT_ALLOW_INSECURE=true)"}

      _ ->
        {:error, "SPILLWAY_WS_EXPORT_URL must be an absolute http(s) URL"}
    end
  end

  defp loopback?("localhost"), do: true

  defp loopback?(host) do
    case :inet.parse_address(String.to_charlist(String.trim(host, "[") |> String.trim("]"))) do
      {:ok, {127, _, _, _}} -> true
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} -> true
      _ -> false
    end
  end

  @doc """
  False, or %{max_pending: n} once the exporter is running, so appends queue
  export copies.
  """
  def enabled?, do: :persistent_term.get({__MODULE__, :on}, false)

  @doc false
  def set_enabled(value), do: :persistent_term.put({__MODULE__, :on}, value)

  @doc "The batch event for one transcript entry (string-keyed envelope)."
  def event(session_id, stream_id, direction, envelope, recorded_at, payloads?) do
    event =
      %{
        "session_id" => session_id,
        "stream_id" => stream_id,
        "direction" => direction,
        "type" => envelope["type"],
        "message_id" => envelope["message_id"],
        "caused_by" => envelope["caused_by"],
        "generation" => envelope["generation"],
        "recorded_at" => recorded_at
      }
      |> Enum.reject(fn {_k, v} -> v in [nil, ""] end)
      |> Map.new()

    if payloads? and Map.has_key?(envelope, "payload"),
      do: Map.put(event, "payload", envelope["payload"]),
      else: event
  end

  defp env_bool(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.downcase(String.trim(value)) in ["1", "true", "yes", "on"]
    end
  end

  defp positive_integer(name, fallback) do
    case Integer.parse(System.get_env(name, "")) do
      {value, ""} when value > 0 -> value
      _ -> fallback
    end
  end

  defp blank_to_nil(nil), do: nil

  defp blank_to_nil(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end
end
