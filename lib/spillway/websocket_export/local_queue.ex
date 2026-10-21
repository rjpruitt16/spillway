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

  @doc """
  Brings the counters up to date with the store. Safe to call on every
  exporter start: the atomics are created once and kept, so sessions that
  are appending during a restart keep drawing unique sequences, and the
  next sequence only ever moves forward.
  """
  def init! do
    ref = counters()

    case Store.last(:ws_export) do
      {<<last::unsigned-big-64>>, _value} -> advance_to(ref, last + 1)
      nil -> advance_to(ref, 1)
    end

    # Exact, not RocksDB's estimate: deletes leave the estimate high until
    # compaction, and enforce_cap would then drop entries still under the cap.
    :atomics.put(ref, 2, Store.count(:ws_export))
    :ok
  end

  defp counters do
    case :persistent_term.get(@counters, nil) do
      nil ->
        ref = :atomics.new(2, signed: false)
        :atomics.put(ref, 1, 1)
        :persistent_term.put(@counters, ref)
        ref

      ref ->
        ref
    end
  end

  # Raises the next sequence to at least floor, never lowering it.
  defp advance_to(ref, floor) do
    current = :atomics.get(ref, 1)

    if current < floor and :atomics.compare_exchange(ref, 1, current, floor) != :ok,
      do: advance_to(ref, floor),
      else: :ok
  end

  @doc "The write op that queues one entry; goes in the transcript's batch."
  def op(session_id, stream_id, direction, envelope, recorded_at) do
    ref = counters()
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
    :atomics.sub(counters(), 2, min(length(keys), pending()))
    :ok
  end

  def pending, do: :atomics.get(counters(), 2)

  @doc """
  Drops the oldest entries beyond max_pending, so a receiver outage can't
  grow the store without bound. Returns how many were dropped. They stay in
  their session transcripts for the replay window.
  """
  def enforce_cap(max_pending) do
    excess = pending() - max_pending

    if excess > 0 do
      # Keys only: the dropped values never need decoding.
      case Store.keys_from(:ws_export, <<0::unsigned-big-64>>, excess) do
        [] ->
          0

        keys ->
          <<last::unsigned-big-64>> = List.last(keys)
          Store.delete_range(:ws_export, hd(keys), <<last + 1::unsigned-big-64>>)
          :atomics.sub(counters(), 2, min(length(keys), pending()))
          length(keys)
      end
    else
      0
    end
  end
end
