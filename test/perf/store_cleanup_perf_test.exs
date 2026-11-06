# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.StoreCleanupPerfTest do
  @moduledoc """
  Issue #59: in soak runs on the RocksDB store, the store grew far faster
  than expiry deleted it. This checks, without a soak, that cleanup keeps up
  with a steady job path: writers insert and complete jobs while cleanup
  expires everything that is due, and the store size must level off instead
  of growing. Run with `make perf` (excluded from `mix test`).

  It doesn't reproduce the soak's 1-CPU node or its HTTP traffic; a pass here
  means cleanup isn't the bottleneck on this machine, not that #59 can't
  happen there.
  """

  use ExUnit.Case, async: false

  alias Spillway.{IdempotentStore, Job}

  @moduletag :perf
  @moduletag timeout: 120_000
  @writers 4
  # 1,000 jobs/s in total, 2-4x the HTTP intake the soak runs reached.
  @per_writer_interval_ms 4
  @duration_ms 20_000

  setup do
    IdempotentStore.clear_ledger()
    on_exit(fn -> IdempotentStore.clear_ledger() end)
  end

  defp write_one(n) do
    job = %Job{
      id: "cleanup-perf-#{n}-#{System.unique_integer([:positive])}",
      user_id: "cleanup-perf",
      idempotent_key: "cleanup-perf-#{n}-#{System.unique_integer([:positive])}",
      url: "https://example.com/work",
      method: "POST",
      headers: %{},
      body: nil,
      webhook_url: "",
      status: :queued,
      created_at: System.system_time(:millisecond)
    }

    :ok = IdempotentStore.check_or_insert(job)
    IdempotentStore.put_result(job.id, %{"status" => 200}, :completed)
    IdempotentStore.finish(job.id, :completed, nil)
  end

  test "cleanup keeps up with a steady job path" do
    stop_at = System.monotonic_time(:millisecond) + @duration_ms
    written = :counters.new(1, [])

    writers =
      for w <- 1..@writers do
        Task.async(fn ->
          Stream.iterate(0, &(&1 + 1))
          |> Enum.reduce_while(0, fn i, _ ->
            if System.monotonic_time(:millisecond) >= stop_at do
              {:halt, :ok}
            else
              write_one(w * 10_000_000 + i)
              :counters.add(written, 1, 1)
              Process.sleep(@per_writer_interval_ms)
              {:cont, :ok}
            end
          end)
        end)
      end

    # Everything completed is due "31 minutes from now", so cleanup always
    # has the whole completed backlog to work through, as when it falls
    # behind in a soak. Like the real loop, a full batch of 2,000 goes
    # straight into the next one; a short batch waits (250 ms here, 10 s in
    # the store).
    deleted = :counters.new(1, [])

    sizes =
      Stream.repeatedly(fn ->
        later = System.system_time(:millisecond) + 31 * 60 * 1_000
        n = IdempotentStore.expire_due(later, 2_000)
        :counters.add(deleted, 1, n)
        if n < 2_000, do: Process.sleep(250)
        IdempotentStore.counts().total_jobs
      end)
      |> Enum.take_while(fn _ -> System.monotonic_time(:millisecond) < stop_at end)

    Enum.each(writers, &Task.await(&1, 30_000))
    total = :counters.get(written, 1)
    rate = total / (@duration_ms / 1_000)
    {first_half, second_half} = Enum.split(sizes, div(length(sizes), 2))
    peak_early = Enum.max(first_half, fn -> 0 end)
    peak_late = Enum.max(second_half, fn -> 0 end)

    IO.puts(
      "cleanup perf: #{total} jobs written (#{round(rate)}/s), #{:counters.get(deleted, 1)} expired, store peak #{peak_early} in the first half, #{peak_late} in the second"
    )

    # At least 10 samples in each half, or there is nothing to compare.
    assert length(sizes) >= 20, "only #{length(sizes)} store samples in #{@duration_ms} ms"

    # Bounded: the late peak stays within half again the early one (plus a
    # little), instead of growing with the run. A sustained cleanup deficit of
    # 50 jobs/s adds about 500 jobs a half and fails this.
    assert peak_late <= peak_early * 1.5 + 100,
           "store kept growing: #{peak_early} early, #{peak_late} late, at #{round(rate)} jobs/s"
  end
end
