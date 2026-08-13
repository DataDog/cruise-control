/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;


/**
 * Compatibility name for the relaxed rack-aware goal.
 *
 * <p>This class keeps configurations written for older Cruise Control versions loadable. The implementation was
 * renamed to {@link RackAwareDistributionGoal}, which provides the same best-effort rack distribution behavior.</p>
 */
public class BestEffortRackAwareGoal extends RackAwareDistributionGoal {
  /**
   * Create a best-effort rack-aware goal.
   */
  public BestEffortRackAwareGoal() {
  }
}
