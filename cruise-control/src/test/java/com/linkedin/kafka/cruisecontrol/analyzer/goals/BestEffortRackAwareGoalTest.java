/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import java.util.List;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class BestEffortRackAwareGoalTest {
  @Test
  public void testLegacyGoalCanBeConfigured() {
    String legacyGoal = BestEffortRackAwareGoal.class.getName();
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.GOALS_CONFIG, legacyGoal);
    properties.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, legacyGoal);
    properties.setProperty(AnalyzerConfig.HARD_GOALS_CONFIG, legacyGoal);
    properties.setProperty(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG, legacyGoal);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(properties);
    List<Goal> configuredGoals = config.getConfiguredInstances(AnalyzerConfig.GOALS_CONFIG, Goal.class);

    assertEquals(1, configuredGoals.size());
    assertEquals(BestEffortRackAwareGoal.class, configuredGoals.get(0).getClass());
    assertTrue(configuredGoals.get(0) instanceof RackAwareDistributionGoal);
  }
}
