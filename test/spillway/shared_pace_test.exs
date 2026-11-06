# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.SharedPaceTest do
  use ExUnit.Case, async: false

  alias Spillway.{AccountQueueRegistry, Job, SharedPace}

  describe "reserve/2" do
    setup do
      domain = "pace-#{System.unique_integer([:positive])}"
      on_exit(fn -> stop(domain) end)
      {:ok, domain: domain}
    end

    test "no pace process means no limit", %{domain: domain} do
      assert SharedPace.reserve(domain, :a) == 0
    end

    test "holds the ceiling and hands slots out in waiting order", %{domain: domain} do
      SharedPace.set_ceiling(domain, 10.0)
      # One interval of credit: the first two slots are free at once.
      assert SharedPace.reserve(domain, :a) == 0
      assert SharedPace.reserve(domain, :a) == 0
      # The next slot is 100 ms away.
      assert SharedPace.reserve(domain, :b) in 90..100
      # :b started waiting first, so a free slot is :b's even if :a asks first.
      Process.sleep(110)
      assert SharedPace.reserve(domain, :a) > 0
      assert SharedPace.reserve(domain, :b) == 0
    end

    test "the advertised rate replaces a static ceiling", %{domain: domain} do
      SharedPace.set_ceiling(domain, 2.0)
      SharedPace.advertise(domain, 100.0)
      assert SharedPace.rate(domain) == 100.0
      assert SharedPace.reserve(domain, :a) == 0
      assert SharedPace.reserve(domain, :a) == 0
      assert SharedPace.reserve(domain, :a) in 1..10
    end

    test "never paces slower than the 0.5 rps floor", %{domain: domain} do
      SharedPace.set_ceiling(domain, 0.01)
      assert SharedPace.reserve(domain, :a) == 0
      assert SharedPace.reserve(domain, :a) == 0
      assert SharedPace.reserve(domain, :a) in 1_900..2_000
    end
  end

  defmodule Target do
    @moduledoc false
    import Plug.Conn

    def init(opts), do: opts

    def call(%Plug.Conn{request_path: "/.well-known/l8"} = conn, _opts),
      do: send_resp(conn, 404, "")

    def call(conn, _opts) do
      if counter = Process.whereis(:shared_pace_hits) do
        "/work/" <> tenant = conn.request_path
        Agent.update(counter, &Map.update(&1, tenant, 1, fn n -> n + 1 end))
      end

      conn
      |> put_resp_header("x-aqueduct-account-queue", "enabled")
      |> put_resp_header("x-aqueduct-rps", "30")
      |> put_resp_header("x-aqueduct-max-concurrent", "4")
      |> send_resp(200, "ok")
    end
  end

  # Three tenants flood one fast target that advertises 30 rps. The target
  # must see about 30 requests a second in total, not about 3 x 30, and each
  # tenant should get about a third (aquifer#146).
  @tag timeout: 60_000
  test "a target's advertised rate holds across tenant queues" do
    {:ok, counter} = Agent.start_link(fn -> %{} end, name: :shared_pace_hits)
    port = 28_000 + rem(System.unique_integer([:positive]), 2_000)
    {:ok, server} = Bandit.start_link(plug: Target, port: port, startup_log: false)

    on_exit(fn ->
      if Process.alive?(server), do: Process.exit(server, :kill)
    end)

    base = "http://localhost:#{port}"

    for n <- 1..400, tenant <- ~w(a b c) do
      job = %Job{
        id: "pace-#{tenant}-#{n}-#{System.unique_integer([:positive])}",
        user_id: "pace-#{tenant}",
        idempotent_key: "pace-#{tenant}-#{n}-#{System.unique_integer([:positive])}",
        url: "#{base}/work/#{tenant}",
        method: "POST",
        headers: %{"Authorization" => "key-#{tenant}"},
        body: "",
        webhook_url: "",
        kind: :user,
        status: :queued,
        created_at: System.system_time(:millisecond)
      }

      AccountQueueRegistry.submit(job, "enabled")
    end

    Process.sleep(2_000)
    Agent.update(counter, fn _ -> %{} end)
    window_ms = 6_000
    Process.sleep(window_ms)
    hits = Agent.get(counter, & &1)
    Agent.stop(counter)

    total = hits |> Map.values() |> Enum.sum()
    rate = total / (window_ms / 1_000)

    IO.puts(
      "delivered #{Float.round(rate, 1)}/s against an advertised 30; per tenant #{inspect(hits)}"
    )

    assert rate <= 36,
           "target received #{rate}/s; the advertised 30 rps should hold across tenants"

    assert rate >= 18, "target received only #{rate}/s; the ceiling should still be usable"

    for tenant <- ~w(a b c) do
      share = Map.get(hits, tenant, 0) / max(total, 1)
      assert share >= 0.2, "tenant #{tenant} got #{round(share * 100)}% (#{inspect(hits)})"
    end
  end

  defp stop(domain) do
    case Registry.lookup(Spillway.SharedPaceRegistry, domain) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(Spillway.SharedPaceSupervisor, pid)
      [] -> :ok
    end
  end
end
