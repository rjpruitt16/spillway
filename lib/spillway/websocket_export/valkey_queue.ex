# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketExport.ValkeyQueue do
  @moduledoc """
  The export queue with Valkey transcripts: one shared stream,
  `spillway:ws:export`, read through the consumer group `spillway-export`,
  so several Spillway nodes share the work. Spillway.WebSocketStore.Valkey
  writes the transcript entry and its export copy in one Lua script
  (append_script/0), so the copy carries the transcript's stream ID.

  Same scheme as Aquifer's WebSocket export.
  """

  @key "spillway:ws:export"
  @group "spillway-export"
  @stale_consumer_ms 3_600_000

  def key, do: @key

  @doc """
  KEYS: transcript, export stream. ARGV: transcript max length, TTL ms,
  export max length, session ID, then the entry's field/value pairs.
  """
  def append_script do
    """
    local fields = {}
    for i = 5, #ARGV do fields[#fields + 1] = ARGV[i] end
    local id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(fields))
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*', 'session_id', ARGV[4], 'stream_id', id, unpack(fields))
    return id
    """
  end

  def ensure_group(conn) do
    case Redix.command(conn, ["XGROUP", "CREATE", @key, @group, "0", "MKSTREAM"]) do
      {:ok, _} -> :ok
      {:error, %Redix.Error{message: "BUSYGROUP" <> _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The next entries for this consumer: its own unacknowledged ones first,
  then ones another consumer left for over a minute, then new ones (waiting
  up to block_ms). Returns {:ok, [{id, entry}]}.
  """
  def read(conn, consumer, limit, block_ms) do
    with {:ok, []} <- read_group(conn, consumer, "0", limit, nil),
         {:ok, []} <- claim_stale(conn, consumer, limit) do
      read_group(conn, consumer, ">", limit, block_ms)
    end
  end

  defp read_group(conn, consumer, id, limit, block_ms) do
    block = if block_ms, do: ["BLOCK", Integer.to_string(block_ms)], else: []

    command =
      ["XREADGROUP", "GROUP", @group, consumer, "COUNT", Integer.to_string(limit)] ++
        block ++ ["STREAMS", @key, id]

    case Redix.command(conn, command, timeout: (block_ms || 0) + 5_000) do
      {:ok, nil} ->
        {:ok, []}

      {:ok, [[_key, entries]]} ->
        {:ok, decode_entries(entries)}

      {:error, %Redix.Error{message: "NOGROUP" <> _}} ->
        with :ok <- ensure_group(conn), do: {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp claim_stale(conn, consumer, limit) do
    command = [
      "XAUTOCLAIM",
      @key,
      @group,
      consumer,
      # Entries another consumer left unacknowledged this long are taken
      # over (a minute; tests lower it).
      Integer.to_string(Application.get_env(:spillway, :ws_export_claim_idle_ms, 60_000)),
      "0-0",
      "COUNT",
      Integer.to_string(limit)
    ]

    case Redix.command(conn, command) do
      {:ok, [_next, entries | _]} ->
        {:ok, decode_entries(entries)}

      {:error, %Redix.Error{message: "NOGROUP" <> _}} ->
        with :ok <- ensure_group(conn), do: {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A pending entry trimmed from the stream (MAXLEN) comes back with no
  # fields. It's returned as {id, nil} so the exporter acknowledges it;
  # skipping it would leave it in the pending list forever.
  defp decode_entries(entries) do
    for [id, fields] <- entries do
      if is_list(fields),
        do: {id, fields |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)},
        else: {id, nil}
    end
  end

  def ack(_conn, []), do: :ok

  def ack(conn, ids) do
    commands = [["MULTI"], ["XACK", @key, @group | ids], ["XDEL", @key | ids], ["EXEC"]]

    case Redix.pipeline(conn, commands) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Removes this node's consumer when it has nothing pending."
  def leave(conn, consumer) do
    case consumers(conn) do
      {:ok, list} ->
        if Enum.any?(list, &(&1["name"] == consumer and &1["pending"] == 0)),
          do: Redix.command(conn, ["XGROUP", "DELCONSUMER", @key, @group, consumer])

        :ok

      _ ->
        :ok
    end
  end

  @doc """
  Removes other consumers with nothing pending that have been idle for an
  hour (nodes that were replaced), so the group doesn't grow with deploys.
  """
  def sweep(conn, consumer) do
    with {:ok, list} <- consumers(conn) do
      for %{"name" => name, "pending" => 0, "idle" => idle} <- list,
          name != consumer,
          idle > @stale_consumer_ms do
        Redix.command(conn, ["XGROUP", "DELCONSUMER", @key, @group, name])
      end

      :ok
    end
  end

  defp consumers(conn) do
    case Redix.command(conn, ["XINFO", "CONSUMERS", @key, @group]) do
      {:ok, list} ->
        {:ok,
         Enum.map(list, fn flat ->
           flat |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
