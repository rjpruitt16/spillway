# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.InboundRateTest do
  # The header test reads the node-wide controller.
  use ExUnit.Case, async: false

  import Phoenix.ConnTest

  alias Spillway.InboundRate

  # One second of inserts at the given latency, then a tick. backlog -1 is
  # "no reading", like Aquifer with no backlog source.
  defp feed(c, inserts, latency_us, opts \\ []) do
    InboundRate.tick(c, %{
      inserts: inserts,
      latencies_us: List.duplicate(latency_us, inserts),
      backlog: Keyword.get(opts, :backlog, -1),
      sync_lag_ms: Keyword.get(opts, :sync_lag_ms, 0),
      active_callers: 1
    })
  end

  defp new, do: InboundRate.new(400, 40)

  test "starts at a percent of the benchmark" do
    assert new().last.advertised_rps == 160.0
  end

  test "ramps fast below the benchmark and carefully above" do
    c = feed(new(), 160, 1_000)
    assert {c.last.advertised_rps, c.last.last_adjustment} == {240.0, "raise"}

    c = feed(%{c | rate: 400.0}, 400, 1_000)
    assert c.last.advertised_rps == 420.0
  end

  test "cuts when insert latency rises" do
    c = feed(new(), 160, 1_000)
    before = c.rate
    c = feed(c, 200, 20_000)
    assert c.last.last_adjustment == "cut: insert latency"
    assert c.last.advertised_rps == Float.round(before * 0.7, 2)
    assert c.last.cuts == 1
  end

  test "doesn't inflate unused headroom" do
    c = feed(new(), 50, 1_000)
    assert {c.last.advertised_rps, c.last.last_adjustment} == {160.0, "hold"}
  end

  test "holds when callers overshoot" do
    c = feed(new(), 300, 1_000)
    assert c.last.last_adjustment == "hold: callers over advertised rate"
    assert c.last.advertised_rps == 160.0
  end

  test "cuts once per episode and learns the knee" do
    c = feed(%{new() | rate: 500.0}, 400, 1_000)
    c = feed(%{c | rate: 500.0}, 450, 20_000)
    assert {c.rate, c.threshold} == {350.0, 450.0}

    c =
      Enum.reduce(1..9, c, fn i, c ->
        c = feed(c, 350, 20_000)
        assert c.rate == 350.0, "tick #{i}: within one episode it holds after the first cut"
        c
      end)

    assert feed(c, 350, 20_000).last.advertised_rps == 245.0
  end

  test "a cascade of cuts keeps the first knee" do
    c = feed(%{new() | rate: 500.0}, 400, 1_000)
    c = feed(%{c | rate: 500.0}, 450, 20_000)
    c = Enum.reduce(1..6, c, fn _, c -> feed(c, 300, 20_000) end)
    assert c.threshold == 450.0
  end

  test "a growing backlog holds, then cuts" do
    c = feed(new(), 160, 1_000, backlog: 0)
    start = c.rate

    {c, _} =
      Enum.reduce(
        ["hold: backlog growing", "hold: backlog growing", "cut: backlog growing"],
        {c, 0},
        fn want, {c, backlog} ->
          backlog = backlog + 500
          c = feed(c, 160, 1_000, backlog: backlog)
          assert c.last.last_adjustment == want
          {c, backlog}
        end
      )

    assert c.rate < start
  end

  test "a steady backlog allows raises" do
    c = feed(new(), 160, 1_000, backlog: 50)
    assert feed(c, 240, 1_000, backlog: 50).last.last_adjustment == "raise"
  end

  # Inserts don't wait for the disk, so they stay fast while it falls
  # behind, and a crash loses everything acknowledged since the last sync.
  # Aquifer's Canalis soak repro (2026-10-27) lost jobs acknowledged 16-37s
  # before a kill this way.
  test "cuts when the disk falls behind, and grows again once it catches up" do
    c = feed(new(), 160, 1_000, sync_lag_ms: 100)
    before = c.rate
    c = feed(c, 200, 1_000, sync_lag_ms: 2_000)
    assert c.last.last_adjustment == "cut: disk behind"
    assert c.last.advertised_rps == Float.round(before * 0.7, 2)
    assert c.last.sync_lag_ms == 2000.0

    c = Enum.reduce(1..3, c, fn _, c -> feed(c, 0, 1_000, sync_lag_ms: 100) end)
    assert feed(c, trunc(c.rate), 1_000, sync_lag_ms: 100).last.last_adjustment == "raise"
  end

  test "splits the rate across active callers" do
    for id <- ["split-a", "split-b", "split-c", "split-d"],
        do: InboundRate.observe(id, 1_000, true)

    rate = InboundRate.snapshot().advertised_rps
    assert_in_delta InboundRate.advertised_rps("split-a"), rate / 4, rate / 4 * 0.5
  end

  test "POST /jobs tells the caller its rate" do
    params = %{
      "user_id" => "inbound-header",
      "idempotent_key" => "inbound-#{System.unique_integer([:positive])}",
      "url" => "https://example.com/work",
      "method" => "POST"
    }

    conn =
      build_conn(:post, "/jobs", params)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Phoenix.ConnTest.dispatch(SpillwayWeb.Endpoint, :post, "/jobs", params)

    assert [rps] = Plug.Conn.get_resp_header(conn, "x-aqueduct-rps")
    assert String.to_float(rps) > 0
    assert Plug.Conn.get_resp_header(conn, "x-aquifer-rps") == [rps]
  end

  # With a longer sync interval, the gap between two healthy syncs is longer
  # than 500ms; that alone isn't the disk falling behind.
  test "allows for a long sync interval" do
    c = feed(InboundRate.new(400, 40, 2_000), 160, 1_000, sync_lag_ms: 1_500)
    assert feed(c, 240, 1_000, sync_lag_ms: 1_500).last.last_adjustment == "raise"
    assert feed(c, 200, 1_000, sync_lag_ms: 5_000).last.last_adjustment == "cut: disk behind"
  end

  test "a direct /proxy response carries the lower of the two rates" do
    lower = &SpillwayWeb.JobController.lower_rps/2
    assert lower.("160.00", "5") == "5"
    assert lower.("2.00", "50") == "2.00"
    assert lower.("160.00", nil) == "160.00"
    assert lower.(nil, "7") == "7"
    assert lower.("160.00", "garbage") == "160.00"
    assert lower.(nil, nil) == nil
  end

  test "caller rows are bounded: an oversized user_id isn't kept" do
    InboundRate.observe(String.duplicate("x", 300), 1_000, true)
    refute :ets.member(:spillway_inbound_callers, String.duplicate("x", 300))
    InboundRate.observe("bounded-caller", 1_000, true)
    assert :ets.member(:spillway_inbound_callers, "bounded-caller")
  end
end
