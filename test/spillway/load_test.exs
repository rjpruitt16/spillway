# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.LoadTest do
  @moduledoc """
  The HTTP user and WebSocket session capacities this node reports to
  Canalis: one CPU budget, shared by whichever kind of traffic uses it.
  Mirrors Aquifer's load_test.go.
  """

  use ExUnit.Case, async: false
  import Plug.Test

  alias Spillway.{Load, Registration}

  @idle %{
    cpu: 0.0,
    cpus: 1,
    job_rate: 0.0,
    message_rate: 0.0,
    http_users: 0,
    ws_sessions: 0,
    max_sessions: 1_000,
    memory_limit_mb: 0,
    memory_mb: 0
  }

  test "an idle node assumes 10 jobs/s per user and 1 message/s per session" do
    # 80% of one CPU / (10 / 700) = 56 users; / (1 / 900) = 720 sessions.
    assert Load.compute(@idle) == %{
             http_users: 0,
             http_capacity: 56,
             ws_sessions: 0,
             ws_capacity: 720
           }
  end

  test "scales with CPUs" do
    assert %{http_capacity: 112, ws_capacity: 1_000} = Load.compute(%{@idle | cpus: 2})
  end

  test "idle sessions leave HTTP users the CPU they don't use" do
    # The Canalis soak's shape: 100 open sessions moving almost nothing,
    # 50 users at 500 jobs/s, half of two CPUs busy.
    load =
      Load.compute(%{
        @idle
        | cpu: 0.5,
          cpus: 2,
          job_rate: 500.0,
          message_rate: 10.0,
          http_users: 50,
          ws_sessions: 100
      })

    assert load.http_capacity > 50 and load.http_capacity < 100
    assert load.ws_capacity == 1_000
  end

  test "busy sessions leave HTTP less room" do
    quiet = Load.compute(%{@idle | cpu: 0.3, job_rate: 100.0, http_users: 10})

    busy =
      Load.compute(%{
        @idle
        | cpu: 0.7,
          job_rate: 100.0,
          message_rate: 600.0,
          http_users: 10,
          ws_sessions: 300
      })

    assert busy.http_capacity < quiet.http_capacity
    assert busy.ws_capacity < 1_000
  end

  test "never reports less than it already has" do
    load = Load.compute(%{@idle | cpu: 1.0, job_rate: 900.0, http_users: 80, ws_sessions: 40})
    assert %{http_capacity: 80, ws_capacity: 40} = load
  end

  test "memory bounds sessions at about 50 KB each" do
    load = Load.compute(%{@idle | memory_limit_mb: 1_000, memory_mb: 990})
    # 10 MB free / 50 KB = 204 sessions.
    assert load.ws_capacity == 204
  end

  test "the session cap bounds sessions" do
    assert %{ws_capacity: 100} = Load.compute(%{@idle | max_sessions: 100})
  end

  test "goes in the registration ping and on every response once read" do
    wait_for_reading()

    payload = Registration.payload()
    assert is_integer(payload.http_capacity) and payload.http_capacity >= payload.http_users
    assert is_integer(payload.ws_capacity) and payload.ws_capacity >= payload.ws_sessions

    conn = conn(:get, "/health") |> SpillwayWeb.Endpoint.call([])

    for header <-
          ~w(x-aqueduct-http-users x-aqueduct-http-capacity x-aqueduct-ws-sessions x-aqueduct-ws-capacity) do
      assert [value] = Plug.Conn.get_resp_header(conn, header)
      assert {_, ""} = Integer.parse(value)
    end
  end

  defp wait_for_reading(tries \\ 50) do
    cond do
      Load.current() -> :ok
      tries == 0 -> flunk("no load reading")
      true -> Process.sleep(100) && wait_for_reading(tries - 1)
    end
  end
end
