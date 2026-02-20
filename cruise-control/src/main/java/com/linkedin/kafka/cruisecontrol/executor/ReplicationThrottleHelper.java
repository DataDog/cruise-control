/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * See https://kafka.apache.org/documentation/#rep-throttle
 */
class ReplicationThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationThrottleHelper.class);
  static final String WILDCARD_ASTERISK = "*";
  static final String LEADER_REPLICATION_THROTTLED_RATE_CONFIG = "leader.replication.throttled.rate";
  static final String FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG = "follower.replication.throttled.rate";
  static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
  static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  static final int RETRIES = 3;
  static final long MAX_DELAY_MS = TimeUnit.SECONDS.toMillis(10);
  private static final long DEFAULT_RETRY_BACKOFF_SCALE_MS = TimeUnit.SECONDS.toMillis(5);
  private static final int DEFAULT_RETRY_BACKOFF_BASE = 2;
  private final AdminClient _adminClient;
  private final Long _throttleRate;
  private final int _retries;
  private long _maxDelayMs;
  private final Set<Integer> _deadBrokers;

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES, MAX_DELAY_MS);
  }

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, Set<Integer> deadBrokers) {
    this(adminClient, throttleRate, RETRIES, MAX_DELAY_MS, deadBrokers);
  }

  // for testing
  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries, long maxDelayMs) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._maxDelayMs = maxDelayMs;
    this._deadBrokers = new HashSet<Integer>();
  }

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries, long maxDelayMs, Set<Integer> deadBrokers) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._maxDelayMs = maxDelayMs;
    this._deadBrokers = deadBrokers;
  }

  void setThrottles(List<ExecutionProposal> replicaMovementProposals)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      LOG.info("Setting a rebalance throttle of {} bytes/sec", _throttleRate);
      Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals);
      // Use batch operations for parallel processing
      setThrottledRateIfNecessaryBatch(participatingBrokers);
      setThrottledReplicasBatch(throttledReplicas);
    }
  }

  // Determines if a candidate task is ready to have its throttles removed.
  boolean shouldRemoveThrottleForTask(ExecutionTask task) {
    return
      // the task should not be in progress
      task.state() != ExecutionTaskState.IN_PROGRESS
      // the task should not be pending
      && task.state() != ExecutionTaskState.PENDING
      // replica throttles only apply to inter-broker replica movement
      && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // determines if a candidate task is in progress and related to inter-broker
  // replica movement.
  boolean taskIsInProgress(ExecutionTask task) {
    return task.state() == ExecutionTaskState.IN_PROGRESS && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // clear throttles for a specific list of execution tasks
  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      List<ExecutionProposal> completedProposals =
        completedTasks
          .stream()
          // Filter for completed tasks related to inter-broker replica movement
          .filter(this::shouldRemoveThrottleForTask)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which have completed a task with
      // inter-broker replica movement
      Set<Integer> participatingBrokers = getParticipatingBrokers(completedProposals);

      List<ExecutionProposal> inProgressProposals =
        inProgressTasks
          .stream()
          .filter(this::taskIsInProgress)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which currently have in-progress
      // inter-broker replica movement
      Set<Integer> brokersWithInProgressTasks = getParticipatingBrokers(inProgressProposals);

      // Remove the brokers with in-progress replica moves from the brokers that have
      // completed inter-broker replica moves
      Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(participatingBrokers);
      brokersToRemoveThrottlesFrom.removeAll(brokersWithInProgressTasks);

      LOG.info("Removing replica movement throttles from brokers in the cluster: {}", brokersToRemoveThrottlesFrom);
      // Use batch operations for parallel processing
      removeThrottledRateFromBrokerBatch(brokersToRemoveThrottlesFrom);

      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(completedProposals);
      removeThrottledReplicasFromTopicBatch(throttledReplicas);
    }
  }

  private boolean throttlingEnabled() {
    return _throttleRate != null;
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionProposal> replicaMovementProposals) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      participatingBrokers.addAll(proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
      participatingBrokers.addAll(proposal.newReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
    }
    participatingBrokers.removeAll(_deadBrokers);
    return participatingBrokers;
  }

  private Map<String, Set<String>> getThrottledReplicasByTopic(List<ExecutionProposal> replicaMovementProposals) {
    Map<String, Set<String>> throttledReplicasByTopic = new HashMap<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      String topic = proposal.topic();
      int partitionId = proposal.partitionId();
      Stream<Integer> brokers = Stream.concat(
        proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId),
        proposal.replicasToAdd().stream().map(ReplicaPlacementInfo::brokerId));
      Set<String> throttledReplicas = throttledReplicasByTopic
        .computeIfAbsent(topic, x -> new TreeSet<>());
      brokers.forEach(brokerId -> throttledReplicas.add(partitionId + ":" + brokerId));
    }
    return throttledReplicasByTopic;
  }

  private void setThrottledRateIfNecessary(int brokerId) throws ExecutionException, InterruptedException, TimeoutException {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }
    Config brokerConfigs = getBrokerConfigs(brokerId);
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleRateConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
      ConfigEntry currThrottleRate = brokerConfigs.get(replicaThrottleRateConfigKey);
      if (currThrottleRate == null || !currThrottleRate.value().equals(String.valueOf(_throttleRate))) {
        LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, _throttleRate, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleRateConfigKey, String.valueOf(_throttleRate)), AlterConfigOp.OpType.SET));
      }
    }
    if (!ops.isEmpty()) {
      changeBrokerConfigs(brokerId, ops);
    }
  }

  private void setThrottledRateIfNecessaryBatch(Set<Integer> brokerIds)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }

    // Step 1: Build all broker ConfigResource objects
    List<ConfigResource> brokerResources = brokerIds.stream()
        .map(id -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)))
        .collect(Collectors.toList());

    // Step 2: Batch fetch all broker configs
    Map<ConfigResource, Config> brokerConfigs = getAllEntityConfigs(brokerResources);

    // Step 3: Determine which brokers need config changes
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (Map.Entry<ConfigResource, Config> entry : brokerConfigs.entrySet()) {
      ConfigResource resource = entry.getKey();
      Config config = entry.getValue();
      List<AlterConfigOp> ops = new ArrayList<>();

      for (String replicaThrottleRateConfigKey : Arrays.asList(
          LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
          FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
        ConfigEntry currThrottleRate = config.get(replicaThrottleRateConfigKey);
        if (currThrottleRate == null || !currThrottleRate.value().equals(String.valueOf(_throttleRate))) {
          LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, _throttleRate, resource.name());
          ops.add(new AlterConfigOp(
              new ConfigEntry(replicaThrottleRateConfigKey, String.valueOf(_throttleRate)),
              AlterConfigOp.OpType.SET));
        }
      }

      if (!ops.isEmpty()) {
        configsToChange.put(resource, ops);
      }
    }

    // Step 4: Batch apply all config changes
    changeAllConfigs(configsToChange);
  }

  private Config getTopicConfigs(String topic) throws ExecutionException, InterruptedException, TimeoutException {
    try {
      return getEntityConfigs(new ConfigResource(ConfigResource.Type.TOPIC, topic));
    } catch (Exception e) {
      if (!topicExists(topic)) {
        return new Config(Collections.emptyList());
      }
      throw e;
    }
  }

  private Config getBrokerConfigs(int brokerId) throws ExecutionException, InterruptedException, TimeoutException {
    return getEntityConfigs(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)));
  }

  private Config getEntityConfigs(ConfigResource cf) throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, Config> configs = _adminClient.describeConfigs(Collections.singletonList(cf)).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return configs.get(cf);
  }

  private Map<ConfigResource, Config> getAllEntityConfigs(Collection<ConfigResource> resources)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (resources.isEmpty()) {
      return Collections.emptyMap();
    }
    return _adminClient.describeConfigs(resources).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private void changeAllConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (configs.isEmpty()) {
      return;
    }
    _adminClient.incrementalAlterConfigs(configs).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // Wait for all configs to be applied
    for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : configs.entrySet()) {
      waitForConfigs(entry.getKey(), entry.getValue());
    }
  }

  private void changeAllTopicConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (configs.isEmpty()) {
      return;
    }

    try {
      _adminClient.incrementalAlterConfigs(configs).all()
          .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      // Check if any topics were deleted during operation
      for (ConfigResource resource : configs.keySet()) {
        if (resource.type() == ConfigResource.Type.TOPIC && !topicExists(resource.name())) {
          LOG.debug("Failed to change configs for topic {} since it does not exist", resource.name());
          // Remove failed topic and retry with remaining
          Map<ConfigResource, Collection<AlterConfigOp>> remaining = new HashMap<>(configs);
          remaining.remove(resource);
          if (!remaining.isEmpty()) {
            changeAllTopicConfigs(remaining);
          }
          return;
        }
      }
      throw e;
    }

    // Wait for all configs to be applied
    for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : configs.entrySet()) {
      waitForConfigs(entry.getKey(), entry.getValue());
    }
  }

  private void setThrottledReplicas(String topic, Set<String> replicas)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config topicConfigs = getTopicConfigs(topic);
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)) {
      ConfigEntry currThrottledReplicas = topicConfigs.get(replicaThrottleConfigKey);
      if (currThrottledReplicas != null && currThrottledReplicas.value().trim().equals(WILDCARD_ASTERISK)) {
        // The existing setup throttles all replica. So, nothing needs to be changed.
        continue;
      }

      // Merge new throttled replicas with existing configuration values.
      Set<String> newThrottledReplicas = new TreeSet<>(replicas);
      if (currThrottledReplicas != null && !currThrottledReplicas.value().equals("")) {
        newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.value().split(",")));
      }
      ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleConfigKey, String.join(",", newThrottledReplicas)), AlterConfigOp.OpType.SET));
    }
    if (!ops.isEmpty()) {
      changeTopicConfigs(topic, ops);
    }
  }

  private void setThrottledReplicasBatch(Map<String, Set<String>> replicasByTopic)
      throws ExecutionException, InterruptedException, TimeoutException {

    // Step 1: Build all topic ConfigResource objects and fetch configs with error handling
    Map<ConfigResource, Config> topicConfigs = new HashMap<>();
    for (String topic : replicasByTopic.keySet()) {
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      try {
        Config config = getTopicConfigs(topic);
        topicConfigs.put(resource, config);
      } catch (Exception e) {
        // Topic might not exist, log and skip
        if (!topicExists(topic)) {
          LOG.debug("Skip setting throttled replicas for topic {} since it does not exist", topic);
        } else {
          throw e;
        }
      }
    }

    // Step 2: Determine which topics need config changes
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : replicasByTopic.entrySet()) {
      String topic = entry.getKey();
      Set<String> replicas = entry.getValue();
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      Config topicConfig = topicConfigs.get(resource);

      if (topicConfig == null) {
        // Topic doesn't exist
        continue;
      }

      List<AlterConfigOp> ops = new ArrayList<>();
      for (String replicaThrottleConfigKey : Arrays.asList(
          LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
          FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)) {
        ConfigEntry currThrottledReplicas = topicConfig.get(replicaThrottleConfigKey);
        if (currThrottledReplicas != null
            && currThrottledReplicas.value().trim().equals(WILDCARD_ASTERISK)) {
          // The existing setup throttles all replica. So, nothing needs to be changed.
          continue;
        }

        // Merge new throttled replicas with existing configuration values.
        Set<String> newThrottledReplicas = new TreeSet<>(replicas);
        if (currThrottledReplicas != null && !currThrottledReplicas.value().equals("")) {
          newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.value().split(",")));
        }
        ops.add(new AlterConfigOp(
            new ConfigEntry(replicaThrottleConfigKey, String.join(",", newThrottledReplicas)),
            AlterConfigOp.OpType.SET));
      }

      if (!ops.isEmpty()) {
        configsToChange.put(resource, ops);
      }
    }

    // Step 3: Batch apply all config changes with error handling
    if (!configsToChange.isEmpty()) {
      changeAllTopicConfigs(configsToChange);
    }
  }

  void changeTopicConfigs(String topic, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    try {
      _adminClient.incrementalAlterConfigs(configs).all()
          .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      waitForConfigs(cf, ops);
    } catch (Exception e) {
      if (!topicExists(topic)) {
        LOG.debug("Failed to change configs for topic {} since it does not exist", topic);
        return;
      }
      throw e;
    }
  }

  void changeBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(cf, ops);
    _adminClient.incrementalAlterConfigs(configs).all()
        .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    waitForConfigs(cf, ops);
  }

  boolean topicExists(String topic) throws InterruptedException, TimeoutException, ExecutionException {
    try {
      return _adminClient.listTopics().names().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).contains(topic);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Unable to check if topic {} exists due to {}", topic, e.getMessage());
      throw e;
    }
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    List<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  /**
   * It gets whether there is any throttled replica specified in the configuration property. If there is and the
   * specified throttled replica does not equal to "*", it modifies the configuration property by removing a
   * given set of replicas from the set of throttled replicas
   *
   * @param topic name of topic which contains <code>replicas</code>
   * @param replicas replicas to remove from the configuration properties
   */
  private void removeThrottledReplicasFromTopic(String topic, Set<String> replicas)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config topicConfigs = getTopicConfigs(topic);
    if (topicConfigs == null) {
      LOG.debug("Skip removing throttled replicas {} from topic {} since no configs can be read", String.join(",", replicas), topic);
      return;
    }
    List<AlterConfigOp> ops = new ArrayList<>();

    ConfigEntry currentLeaderThrottledReplicas = topicConfigs.get(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentLeaderThrottledReplicas != null) {
      if (currentLeaderThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle");
      } else {
        replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentLeaderThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    ConfigEntry currentFollowerThrottledReplicas = topicConfigs.get(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentFollowerThrottledReplicas != null) {
      if (currentFollowerThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle");
      } else {
        replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentFollowerThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    if (!ops.isEmpty()) {
      changeTopicConfigs(topic, ops);
    }
  }

  private void removeThrottledReplicasFromTopicBatch(Map<String, Set<String>> replicasByTopic)
      throws ExecutionException, InterruptedException, TimeoutException {

    // Step 1: Build all topic ConfigResource objects and fetch configs
    Map<ConfigResource, Config> topicConfigs = new HashMap<>();
    for (String topic : replicasByTopic.keySet()) {
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      try {
        Config config = getTopicConfigs(topic);
        if (config != null) {
          topicConfigs.put(resource, config);
        }
      } catch (Exception e) {
        LOG.debug("Skip removing throttled replicas from topic {} since no configs can be read", topic);
      }
    }

    // Step 2: Determine which topics need replica throttle removal
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : replicasByTopic.entrySet()) {
      String topic = entry.getKey();
      Set<String> replicas = entry.getValue();
      ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      Config topicConfig = topicConfigs.get(resource);

      if (topicConfig == null) {
        LOG.debug("Skip removing throttled replicas {} from topic {} since no configs can be read",
            String.join(",", replicas), topic);
        continue;
      }

      List<AlterConfigOp> ops = new ArrayList<>();

      ConfigEntry currentLeaderThrottledReplicas = topicConfig.get(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
      if (currentLeaderThrottledReplicas != null) {
        if (currentLeaderThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
          LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle");
        } else {
          replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} and replica {}", topic, r));
          String newThrottledReplicas = removeReplicasFromConfig(currentLeaderThrottledReplicas.value(), replicas);
          if (newThrottledReplicas.isEmpty()) {
            ops.add(new AlterConfigOp(
                new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null),
                AlterConfigOp.OpType.DELETE));
          } else {
            ops.add(new AlterConfigOp(
                new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas),
                AlterConfigOp.OpType.SET));
          }
        }
      }

      ConfigEntry currentFollowerThrottledReplicas = topicConfig.get(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
      if (currentFollowerThrottledReplicas != null) {
        if (currentFollowerThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
          LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle");
        } else {
          replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
          String newThrottledReplicas = removeReplicasFromConfig(currentFollowerThrottledReplicas.value(), replicas);
          if (newThrottledReplicas.isEmpty()) {
            ops.add(new AlterConfigOp(
                new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null),
                AlterConfigOp.OpType.DELETE));
          } else {
            ops.add(new AlterConfigOp(
                new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas),
                AlterConfigOp.OpType.SET));
          }
        }
      }

      if (!ops.isEmpty()) {
        configsToChange.put(resource, ops);
      }
    }

    // Step 3: Batch apply all config removals
    if (!configsToChange.isEmpty()) {
      changeAllTopicConfigs(configsToChange);
    }
  }

  private void removeThrottledRateFromBroker(Integer brokerId)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config brokerConfigs = getBrokerConfigs(brokerId);
    ConfigEntry currLeaderThrottle = brokerConfigs.get(LEADER_REPLICATION_THROTTLED_RATE_CONFIG);
    ConfigEntry currFollowerThrottle = brokerConfigs.get(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG);
    List<AlterConfigOp> ops = new ArrayList<>();
    if (currLeaderThrottle != null) {
      if (currLeaderThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
          || currLeaderThrottle.source().equals(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)) {
        LOG.debug("Skipping removal for global leader throttle rate: {}", currFollowerThrottle);
      } else {
        LOG.debug("Removing leader throttle rate: {} on broker {}", currLeaderThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, null), AlterConfigOp.OpType.DELETE));
      }
    }
    if (currFollowerThrottle != null) {
      if (currFollowerThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
          || currFollowerThrottle.source().equals(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)) {
        LOG.debug("Skipping removal for global follower throttle rate: {}", currFollowerThrottle);
      } else {
        LOG.debug("Removing follower throttle rate: {} on broker {}", currFollowerThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, null), AlterConfigOp.OpType.DELETE));
      }
    }
    if (!ops.isEmpty()) {
      changeBrokerConfigs(brokerId, ops);
    }
  }

  private void removeThrottledRateFromBrokerBatch(Set<Integer> brokerIds)
      throws ExecutionException, InterruptedException, TimeoutException {

    // Step 1: Build all broker ConfigResource objects
    List<ConfigResource> brokerResources = brokerIds.stream()
        .map(id -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)))
        .collect(Collectors.toList());

    // Step 2: Batch fetch all broker configs
    Map<ConfigResource, Config> brokerConfigs = getAllEntityConfigs(brokerResources);

    // Step 3: Determine which brokers need throttle removal
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (Map.Entry<ConfigResource, Config> entry : brokerConfigs.entrySet()) {
      ConfigResource resource = entry.getKey();
      Config config = entry.getValue();
      List<AlterConfigOp> ops = new ArrayList<>();

      ConfigEntry currLeaderThrottle = config.get(LEADER_REPLICATION_THROTTLED_RATE_CONFIG);
      ConfigEntry currFollowerThrottle = config.get(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG);

      if (currLeaderThrottle != null) {
        if (currLeaderThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
            || currLeaderThrottle.source().equals(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)) {
          LOG.debug("Skipping removal for global leader throttle rate: {}", currFollowerThrottle);
        } else {
          LOG.debug("Removing leader throttle rate: {} on broker {}", currLeaderThrottle, resource.name());
          ops.add(new AlterConfigOp(
              new ConfigEntry(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, null),
              AlterConfigOp.OpType.DELETE));
        }
      }
      if (currFollowerThrottle != null) {
        if (currFollowerThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
            || currFollowerThrottle.source().equals(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)) {
          LOG.debug("Skipping removal for global follower throttle rate: {}", currFollowerThrottle);
        } else {
          LOG.debug("Removing follower throttle rate: {} on broker {}", currFollowerThrottle, resource.name());
          ops.add(new AlterConfigOp(
              new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, null),
              AlterConfigOp.OpType.DELETE));
        }
      }

      if (!ops.isEmpty()) {
        configsToChange.put(resource, ops);
      }
    }

    // Step 4: Batch apply all config removals
    changeAllConfigs(configsToChange);
  }

  // Retries until we can read the configs changes we just wrote
  void waitForConfigs(ConfigResource cf, Collection<AlterConfigOp> ops) {
    // Use HashMap::new instead of Collectors.toMap to allow inserting null values
    Map<String, String> expectedConfigs = ops.stream()
            .collect(HashMap::new, (m, o) -> m.put(o.configEntry().name(), o.configEntry().value()), HashMap::putAll);
    boolean retryResponse = CruiseControlMetricsUtils.retry(() -> {
      try {
        return !configsEqual(getEntityConfigs(cf), expectedConfigs);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        return false;
      }
    }, DEFAULT_RETRY_BACKOFF_SCALE_MS, DEFAULT_RETRY_BACKOFF_BASE, _retries, (int) _maxDelayMs);
    if (!retryResponse) {
      throw new IllegalStateException("The following configs " + ops + " were not applied to " + cf + " within the time limit");
    }
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry configEntry = configs.get(entry.getKey());
      if (configEntry == null || configEntry.value() == null || configEntry.value().isEmpty()) {
        if (entry.getValue() != null) {
          return false;
        }
      } else if ((configEntry.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
          || configEntry.source().equals(ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)) && entry.getValue() == null) {
        LOG.debug("Found global broker config: {}, skipping comparison", configEntry);
      } else if (!Objects.equals(entry.getValue(), configEntry.value())) {
        return false;
      }
    }
    return true;
  }
}
