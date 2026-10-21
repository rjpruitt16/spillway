# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketExport.LocalQueue do
  @moduledoc """
  The export queue with the local store: the :ws_export column family,
  keyed by a 64-bit sequence so key order is append order. The append path
  adds an entry in the same RocksDB batch as the transcript entry
  (Spillway.WebSocketStore), and the exporter deletes entries after a 2xx.
  """

  alias Spillway.Store

  @counters {__MODULE__, :counters}
  # counters: 1 = next sequence, 2 = entries pending (approximate)

  @doc "Sets the sequence after the newest queued entry. Called at boot."
  def init! do
    ref = :atomics.new(2, signed: false)

    case Store.last(:ws_export) do
      {<<last::unsigned-big-64>>, _value} -> :atomics.put(ref, 1, last + 1)
      nil -> :atomics.put(ref, 1, 1)
    end

    estimate =
      case Store.property(:ws_export, "rocksdb.estimate-num-keys") do
        nil -> 0
        value -> String.to_integer(to_string(value))
      end

    :atomics.put(ref, 2, estimate)
    :persistent_term.put(@counters, ref)
    :ok
  end

  @doc "The write op that queues one entry; goes in the transcript's batch."
  def op(session_id, stream_id, direction, envelope, recorded_at) do
    ref = :persistent_term.get(@counters)
    sequence = :atomics.add_get(ref, 1, 1) - 1
    :atomics.add(ref, 2, 1)

    {:put, :ws_export, <<sequence::unsigned-big-64>>,
     {session_id, stream_id, direction, envelope, recorded_at}}
  end

  @doc "Up to limit queued entries, oldest first, as {key, entry}."
  def read(limit) do
    Store.take_from(:ws_export, <<0::unsigned-big-64>>, limit, fn _ -> true end)
  end

  @doc "Deletes acknowledged entries."
  def ack([]), do: :ok

  def ack(keys) do
    Store.write(Enum.map(keys, &{:delete, :ws_export, &1}))
    :atomics.sub(:persistent_term.get(@counters), 2, min(length(keys), pending()))
    :ok
  end

  def pending, do: :atomics.get(:persistent_term.get(@counters), 2)

  @doc """
  Drops the oldest entries beyond max_pending, so a receiver outage can't
  grow the store without bound. Returns how many were dropped. They stay in
  their session transcripts for the replay window.
  """
  def enforce_cap(max_pending) do
    excess = pending() - max_pending

    if excess > 0 do
      case read(excess) do
        [] ->
          0

        entries ->
          {first, _} = hd(entries)
          {<<last::unsigned-big-64>>, _} = List.last(entries)
          Store.delete_range(:ws_export, first, <<last + 1::unsigned-big-64>>)
          :atomics.sub(:persistent_term.get(@counters), 2, min(length(entries), pending()))
          length(entries)
      end
    else
      0
    end
  end
end
