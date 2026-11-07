# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.SharedPaceTest do
  use ExUnit.Case, async: false

  alias Spillway.{AccountQueueRegistry, Job, SharedPace}

  describe "reserve and the total" do
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

    test "the highest rate advertised to an active queue is the total", %{domain: domain} do
      SharedPace.set_ceiling(domain, 2.0)
      assert SharedPace.rate(domain) == 2.0
      SharedPace.reserve_slot(domain, :vip, 1000.0, 1000.0, 0)
      SharedPace.reserve_slot(domain, :free, 10.0, 10.0, 0)
      assert SharedPace.rate(domain) == 1000.0
      # A queue that sent nothing doesn't lower it.
      SharedPace.reserve_slot(domain, :quiet, 2.0, nil, 0)
      assert SharedPace.rate(domain) == 1000.0
    end

    test "per-tenant rates don't flip the total (the old latest-wins bug)", %{domain: domain} do
      SharedPace.set_ceiling(domain, 2.0)

      for _ <- 1..5 do
        SharedPace.reserve_slot(domain, :vip, 50.0, 50.0, 0)
        SharedPace.reserve_slot(domain, :free, 1.0, 1.0, 0)
        assert SharedPace.rate(domain) == 50.0
      end
    end

    test "the total drops as soon as the top queue lowers its rate", %{domain: domain} do
      SharedPace.set_ceiling(domain, 2.0)
      SharedPace.reserve_slot(domain, :emergency, 1000.0, 1000.0, 0)
      SharedPace.reserve_slot(domain, :other, 10.0, 10.0, 0)
      assert SharedPace.rate(domain) == 1000.0
      SharedPace.reserve_slot(domain, :emergency, 10.0, 10.0, 0)
      assert SharedPace.rate(domain) == 10.0
    end

    test "a promoted queue gets its new share at once", %{domain: domain} do
      SharedPace.set_ceiling(domain, 60.0)
      # low takes a slot at 0.5, a step of 2, and is then promoted to 600.
      assert {:ok, _} = SharedPace.reserve_slot(domain, :low, 0.5, 0.5, 0)
      # Each with its own interval at 600/s, like a real queue.
      got = contend(domain, %{low: 600.0, busy: 600.0}, 1_000, 1_667)
      assert Map.get(got, :low, 0) >= 20, "promoted queue got #{inspect(got)} in its first second"
    end

    test "a target advertising less than configured lowers the total", %{domain: domain} do
      SharedPace.set_ceiling(domain, 20.0)
      SharedPace.reserve_slot(domain, :a, 1.0, 1.0, 0)
      assert SharedPace.rate(domain) == 1.0
    end

    test "a new owner takes over, and the old one going down doesn't stop the pace",
         %{domain: domain} do
      old = spawn(fn -> Process.sleep(:infinity) end)
      new = spawn(fn -> Process.sleep(:infinity) end)
      SharedPace.set_ceiling(domain, 5.0, old)
      SharedPace.set_ceiling(domain, 7.0, new)
      Process.exit(old, :kill)
      Process.sleep(50)
      assert SharedPace.rate(domain) == 7.0

      Process.exit(new, :kill)
      Process.sleep(50)
      assert SharedPace.rate(domain) == nil
    end

    test "never paces slower than the 0.5 rps floor", %{domain: domain} do
      SharedPace.set_ceiling(domain, 0.01)
      assert SharedPace.reserve(domain, :a) == 0
      assert SharedPace.reserve(domain, :a) == 0
      assert SharedPace.reserve(domain, :a) in 1_900..2_000
    end
  end

  describe "weighted turns" do
    setup do
      domain = "pace-#{System.unique_integer([:positive])}"
      on_exit(fn -> stop(domain) end)
      {:ok, domain: domain}
    end

    # Wildly unfair on purpose: the target advertises 200, 20 and 2. Three
    # always-backlogged queues ask as soon as they're told; the total is 200/s
    # and they should get 90.1%, 9.0% and 0.9%.
    @tag timeout: 30_000
    test "slots split in proportion to the advertised rates", %{domain: domain} do
      SharedPace.set_ceiling(domain, 2.0)
      weights = %{vip: 200.0, standard: 20.0, low: 2.0}
      got = contend(domain, weights, 5_000)
      total = got |> Map.values() |> Enum.sum()
      IO.puts("weighted turns: #{inspect(got)} in 5 s")
      assert total in 900..1_010, "want about 1000 slots at 200/s, got #{total}"

      for {name, w} <- weights do
        want = total * w / 222

        assert abs(got[name] - want) <= max(want * 0.1, 3),
               "#{name} took #{got[name]}, want about #{round(want)} (#{inspect(got)})"
      end
    end
  end

  # Each queue in weights asks for a slot (advertising its weight) whenever
  # it's told to, for duration_ms of wall time; returns the slots each took.
  defp contend(domain, weights, duration_ms, own_us \\ 0) do
    deadline = System.monotonic_time(:millisecond) + duration_ms

    weights
    |> Enum.map(fn {name, w} ->
      Task.async(fn ->
        {name, ask_until(domain, name, w, own_us, deadline, 0)}
      end)
    end)
    |> Task.await_many(duration_ms + 5_000)
    |> Map.new()
  end

  defp ask_until(domain, name, w, own_us, deadline, n) do
    if System.monotonic_time(:millisecond) >= deadline do
      n
    else
      case SharedPace.reserve_slot(domain, name, w, w, own_us) do
        {:ok, _age} ->
          if own_us > 0, do: Process.sleep(max(div(own_us, 1_000), 1))
          ask_until(domain, name, w, own_us, deadline, n + 1)

        {:wait, ms} ->
          Process.sleep(ms)
          ask_until(domain, name, w, own_us, deadline, n)
      end
    end
  end

  defmodule WeightedTarget do
    @moduledoc false
    import Plug.Conn

    def init(opts), do: opts

    def call(%Plug.Conn{request_path: "/.well-known/l8"} = conn, _opts),
      do: send_resp(conn, 404, "")

    def call(conn, _opts) do
      "/work/" <> tenant = conn.request_path
      {rates, counter} = :persistent_term.get(:weighted_target)
      if counter, do: Agent.update(counter, &Map.update(&1, tenant, 1, fn n -> n + 1 end))

      conn
      |> put_resp_header("x-aqueduct-account-queue", "enabled")
      |> put_resp_header("x-aqueduct-rps", to_string(Map.fetch!(rates, tenant)))
      |> put_resp_header("x-aqueduct-max-concurrent", "4")
      |> send_resp(200, "ok")
    end
  end

  # End to end through real account queues: an unconfigured target (2 rps
  # default) advertises wildly different rates to three tenants, 60, 6 and
  # 0.6. The total must rise to 60/s and split 90.0/9.0/0.9%. Then the target
  # swaps the first and last, and the shares must follow.
  @tag timeout: 90_000
  test "a target sets its total and each tenant's share by what it advertises" do
    rates = %{"vip" => 60.0, "standard" => 6.0, "low" => 0.6}
    swapped = %{"vip" => 0.6, "standard" => 6.0, "low" => 60.0}
    :persistent_term.put(:weighted_target, {rates, nil})
    port = 30_000 + rem(System.unique_integer([:positive]), 2_000)
    {:ok, server} = Bandit.start_link(plug: WeightedTarget, port: port, startup_log: false)
    base = "http://localhost:#{port}"

    on_exit(fn ->
      case :ets.lookup(:url_actors, base) do
        [{_, actor}] ->
          queues = :sys.get_state(actor).queues |> Map.values()
          Enum.each([actor | queues], &Process.exit(&1, :kill))

        [] ->
          :ok
      end

      if Process.alive?(server), do: Process.exit(server, :kill)
      :persistent_term.erase(:weighted_target)
      Spillway.IdempotentStore.clear_ledger()
    end)

    # Enough that every queue stays backlogged through both phases (the
    # leader takes about 54/s for 10 s in its phase).
    for n <- 1..700, tenant <- ~w(vip standard low) do
      key = "w-#{tenant}-#{n}-#{System.unique_integer([:positive])}"

      AccountQueueRegistry.submit(
        %Job{
          id: key,
          user_id: "w-#{tenant}",
          idempotent_key: key,
          url: "#{base}/work/#{tenant}",
          method: "POST",
          headers: %{"Authorization" => "key-#{tenant}"},
          body: "",
          webhook_url: "",
          kind: :user,
          status: :queued,
          created_at: System.system_time(:millisecond)
        },
        "enabled"
      )
    end

    measure = fn rates ->
      {:ok, counter} = Agent.start_link(fn -> %{} end)
      :persistent_term.put(:weighted_target, {rates, nil})
      Process.sleep(2_000)
      :persistent_term.put(:weighted_target, {rates, counter})
      Process.sleep(8_000)
      :persistent_term.put(:weighted_target, {rates, nil})
      hits = Agent.get(counter, & &1)
      Agent.stop(counter)
      hits
    end

    for {phase, r} <- [{1, rates}, {2, swapped}] do
      hits = measure.(r)
      total = hits |> Map.values() |> Enum.sum()
      rate = total / 8

      IO.puts(
        "phase #{phase}: #{Float.round(rate, 1)}/s, #{inspect(hits)} (advertised #{inspect(r)})"
      )

      assert rate >= 51 and rate <= 66, "phase #{phase}: #{rate}/s; want about the advertised 60"
      sum = r |> Map.values() |> Enum.sum()

      for {tenant, w} <- r do
        want = total * w / sum
        got = Map.get(hits, tenant, 0)

        assert abs(got - want) <= max(want * 0.15, 4),
               "phase #{phase}: #{tenant} got #{got} of #{total}, want about #{round(want)}"
      end
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

    base = "http://localhost:#{port}"

    # Stop this test's queues before the server, so nothing keeps retrying
    # against it into later tests, and drop its jobs.
    on_exit(fn ->
      case :ets.lookup(:url_actors, base) do
        [{_, actor}] ->
          queues = :sys.get_state(actor).queues |> Map.values()
          Enum.each([actor | queues], &Process.exit(&1, :kill))

        [] ->
          :ok
      end

      if Process.alive?(server), do: Process.exit(server, :kill)
      Spillway.IdempotentStore.clear_ledger()
    end)

    # 150 per tenant keeps every queue backlogged through the 8 s measured
    # (about 10/s each).
    for n <- 1..150, tenant <- ~w(a b c) do
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
