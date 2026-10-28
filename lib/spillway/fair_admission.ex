# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.FairAdmission do
  @moduledoc false

  @default_max_pending_per_upstream 10_000
  @pressure_start 0.70
  # How far past its budget an upstream's backlog may grow while queues at or
  # below their fair share keep entering. Past it, every request is refused.
  @fair_ceiling 1.25

  def max_pending_per_upstream do
    case Integer.parse(
           System.get_env(
             "SPILLWAY_MAX_PENDING_PER_UPSTREAM",
             Integer.to_string(@default_max_pending_per_upstream)
           )
         ) do
      {value, ""} when value >= 0 -> value
      _ -> @default_max_pending_per_upstream
    end
  end

  def decide(queue_pending, total_pending, active_queues, max_backlog, draw \\ :rand.uniform()) do
    active_queues = max(active_queues, 1)
    projected_queue = queue_pending + 1
    projected_total = total_pending + 1

    snapshot = %{
      active_queues: active_queues,
      upstream_backlog: projected_total,
      queue_backlog: projected_queue,
      max_backlog: max_backlog,
      admission_pressure: 0.0,
      rejection_probability: 0.0
    }

    cond do
      max_backlog <= 0 ->
        {:allowed, snapshot}

      # Past the budget only queues above their fair share are refused, so one
      # large queue filling the budget doesn't lock out small ones.
      projected_total > max_backlog and active_queues > 1 and
        projected_queue <= max_backlog / active_queues and
          projected_total <= @fair_ceiling * max_backlog ->
        {:allowed, %{snapshot | admission_pressure: 1.0}}

      projected_total > max_backlog ->
        {:rejected, "upstream_queue",
         %{snapshot | admission_pressure: 1.0, rejection_probability: 1.0}}

      active_queues == 1 ->
        {:allowed, snapshot}

      true ->
        start = @pressure_start * max_backlog
        pressure = clamp((projected_total - start) / (max_backlog - start))
        fair_share = max_backlog / active_queues
        # A queue at twice its fair share or more is fully in excess, so at
        # full pressure it is refused every time, before the budget fills.
        excess = clamp((projected_queue - fair_share) / fair_share)
        probability = pressure * excess

        snapshot = %{
          snapshot
          | admission_pressure: pressure,
            rejection_probability: probability
        }

        if probability > 0 and clamp(draw) < probability do
          {:rejected, "queue_fair_share", snapshot}
        else
          {:allowed, snapshot}
        end
    end
  end

  def pressure(backlog, max_backlog) when max_backlog > 0 do
    start = @pressure_start * max_backlog
    clamp((backlog - start) / (max_backlog - start))
  end

  def pressure(_backlog, _max_backlog), do: 0.0

  defp clamp(value), do: max(0.0, min(value * 1.0, 1.0))
end
