# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.FairAdmissionTest do
  use ExUnit.Case, async: true

  alias Spillway.FairAdmission

  test "single active queue borrows the whole budget" do
    assert {:allowed, snapshot} = FairAdmission.decide(99, 99, 1, 100, 0.0)
    assert snapshot.queue_backlog == 100
    assert snapshot.upstream_backlog == 100
  end

  test "global budget rejects only after it is full" do
    assert {:rejected, "upstream_queue", snapshot} =
             FairAdmission.decide(100, 100, 1, 100, 0.99)

    assert snapshot.rejection_probability == 1.0
  end

  test "pressure rejects the noisy queue without rejecting the quiet queue" do
    assert {:rejected, "queue_fair_share", noisy} =
             FairAdmission.decide(89, 94, 2, 100, 0.0)

    assert noisy.rejection_probability > 0
    assert {:allowed, _quiet} = FairAdmission.decide(5, 94, 2, 100, 0.0)
  end

  test "unused capacity remains borrowable below the pressure threshold" do
    assert {:allowed, snapshot} = FairAdmission.decide(60, 60, 2, 100, 0.0)
    assert snapshot.admission_pressure == 0.0
  end

  test "rejection becomes less aggressive as competitors leave" do
    {:allowed, four_queues} = FairAdmission.decide(79, 89, 4, 100, 0.99)
    {:allowed, two_queues} = FairAdmission.decide(79, 89, 2, 100, 0.99)

    assert four_queues.rejection_probability > two_queues.rejection_probability
  end

  test "a full budget still admits queues at or below their fair share" do
    # One queue holds the whole budget; a small one keeps entering.
    assert {:allowed, small} = FairAdmission.decide(3, 100, 2, 100, 0.99)
    assert small.admission_pressure == 1.0
    assert {:rejected, "upstream_queue", _} = FairAdmission.decide(97, 100, 2, 100, 0.0)
  end

  test "past 125% of the budget every request is refused" do
    assert {:allowed, _} = FairAdmission.decide(10, 123, 2, 100, 0.99)
    assert {:rejected, "upstream_queue", _} = FairAdmission.decide(10, 125, 2, 100, 0.0)
  end

  test "a queue at twice its fair share is refused every time at full pressure" do
    # F = 50; the queue would hold 100 with the budget about to fill.
    assert {:rejected, "queue_fair_share", snapshot} =
             FairAdmission.decide(99, 99, 2, 100, 0.999)

    assert snapshot.rejection_probability == 1.0
  end
end
