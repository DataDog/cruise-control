/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
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
  static final String LEADER_THROTTLED_RATE = "leader.replication.throttled.rate";
  static final String FOLLOWER_THROTTLED_RATE = "follower.replication.throttled.rate";
  // LogConfig class in Kafka 3.5+
  private static final String LOG_CONFIG_IN_KAFKA_3_5_AND_LATER = "org.apache.kafka.storage.internals.log.LogConfig";
  // LogConfig class in Kafka 3.4-
  private static final String LOG_CONFIG_IN_KAFKA_3_4_AND_EARLIER = "kafka.log.LogConfig";
  static final String LEADER_THROTTLED_REPLICAS = getLogConfig(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
  static final String FOLLOWER_THROTTLED_REPLICAS = getLogConfig(LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  private static final Config EMPTY_CONFIG = new Config(Collections.emptyList());
  private static final long INITIAL_CONFIG_CHANGE_CHECK_INTERVAL_MS = 250L;
  private static final long MAX_CONFIG_CHANGE_CHECK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  static final int RETRIES = 30;

  private final AdminClient _adminClient;
  private final Long _throttleRate;
  private final int _retries;
  private final Set<Integer> _deadBrokers;

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES);
  }

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, Set<Integer> deadBrokers) {
    this(adminClient, throttleRate, RETRIES, deadBrokers);
  }

  // for testing
  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._deadBrokers = new HashSet<Integer>();
  }

  ReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries, Set<Integer> deadBrokers) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._deadBrokers = deadBrokers;
  }

  void setThrottles(List<ExecutionProposal> replicaMovementProposals)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      LOG.info("Setting a rebalance throttle of {} bytes/sec", _throttleRate);
      Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals);
      setThrottledRateIfNecessary(participatingBrokers);
      setThrottledReplicas(throttledReplicas);
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
      removeThrottledRateFromBrokers(brokersToRemoveThrottlesFrom);

      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(completedProposals);
      removeThrottledReplicasFromTopics(throttledReplicas);
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

  private void setThrottledRateIfNecessary(Set<Integer> brokerIds) throws ExecutionException, InterruptedException, TimeoutException {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }
    if (brokerIds.isEmpty()) {
      return;
    }

    List<ConfigResource> brokerResources = brokerConfigResources(brokerIds);
    Map<ConfigResource, Config> brokerConfigsByResource = getEntityConfigs(brokerResources, false);
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (ConfigResource brokerResource : brokerResources) {
      Config brokerConfigs = brokerConfigsByResource.get(brokerResource);
      List<AlterConfigOp> ops = new ArrayList<>();
      for (String replicaThrottleRateConfigKey : Arrays.asList(LEADER_THROTTLED_RATE, FOLLOWER_THROTTLED_RATE)) {
        ConfigEntry currThrottleRate = brokerConfigs.get(replicaThrottleRateConfigKey);
        if (currThrottleRate == null || !currThrottleRate.value().equals(String.valueOf(_throttleRate))) {
          LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, _throttleRate, brokerResource.name());
          ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleRateConfigKey, String.valueOf(_throttleRate)), AlterConfigOp.OpType.SET));
        }
      }
      if (!ops.isEmpty()) {
        configsToChange.put(brokerResource, ops);
      }
    }
    changeConfigs(configsToChange);
  }

  private Map<ConfigResource, Config> getEntityConfigs(Collection<ConfigResource> resources, boolean ignoreMissingTopics)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (resources.isEmpty()) {
      return Collections.emptyMap();
    }

    DescribeConfigsResult describeConfigsResult = _adminClient.describeConfigs(resources);
    Map<ConfigResource, KafkaFuture<Config>> configFutures = describeConfigsResult.values();
    Map<ConfigResource, Config> configsByResource = new HashMap<>();
    for (ConfigResource resource : resources) {
      KafkaFuture<Config> configFuture = configFutures.get(resource);
      if (configFuture == null) {
        throw new IllegalStateException("Failed to retrieve config future for resource " + resource + ".");
      }
      try {
        configsByResource.put(resource, configFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      } catch (ExecutionException | TimeoutException e) {
        if (ignoreMissingTopics && resource.type() == ConfigResource.Type.TOPIC && !topicExists(resource.name())) {
          continue;
        }
        throw e;
      }
    }
    return configsByResource;
  }

  private Map<ConfigResource, Config> getEntityConfigsForTopics(Collection<String> topics)
      throws ExecutionException, InterruptedException, TimeoutException {
    return getEntityConfigs(topicConfigResources(topics), true);
  }

  private void setThrottledReplicas(Map<String, Set<String>> throttledReplicasByTopic)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (throttledReplicasByTopic.isEmpty()) {
      return;
    }

    Map<ConfigResource, Config> topicConfigsByResource = getEntityConfigsForTopics(throttledReplicasByTopic.keySet());
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : throttledReplicasByTopic.entrySet()) {
      ConfigResource topicResource = topicConfigResource(entry.getKey());
      Config topicConfigs = topicConfigsByResource.getOrDefault(topicResource, EMPTY_CONFIG);
      List<AlterConfigOp> ops = new ArrayList<>();
      for (String replicaThrottleConfigKey : Arrays.asList(LEADER_THROTTLED_REPLICAS, FOLLOWER_THROTTLED_REPLICAS)) {
        ConfigEntry currThrottledReplicas = topicConfigs.get(replicaThrottleConfigKey);
        if (currThrottledReplicas != null && currThrottledReplicas.value().trim().equals(WILDCARD_ASTERISK)) {
          // The existing setup throttles all replica. So, nothing needs to be changed.
          continue;
        }

        // Merge new throttled replicas with existing configuration values.
        Set<String> newThrottledReplicas = new TreeSet<>(entry.getValue());
        if (currThrottledReplicas != null && !currThrottledReplicas.value().equals("")) {
          newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.value().split(",")));
        }
        ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleConfigKey, String.join(",", newThrottledReplicas)),
                                  AlterConfigOp.OpType.SET));
      }
      if (!ops.isEmpty()) {
        configsToChange.put(topicResource, ops);
      }
    }
    changeConfigs(configsToChange);
  }

  void changeTopicConfigs(String topic, Collection<AlterConfigOp> ops)
      throws ExecutionException, InterruptedException, TimeoutException {
    changeConfigs(Collections.singletonMap(topicConfigResource(topic), ops));
  }

  void changeBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops)
      throws ExecutionException, InterruptedException, TimeoutException {
    changeConfigs(Collections.singletonMap(brokerConfigResource(brokerId), ops));
  }

  private void changeConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configsByResource)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (configsByResource.isEmpty()) {
      return;
    }

    AlterConfigsResult alterConfigsResult = _adminClient.incrementalAlterConfigs(configsByResource);
    Map<ConfigResource, KafkaFuture<Void>> configFutures = alterConfigsResult.values();
    Map<ConfigResource, Collection<AlterConfigOp>> appliedConfigs = new HashMap<>();
    for (ConfigResource resource : orderedConfigResources(configsByResource.keySet())) {
      KafkaFuture<Void> configFuture = configFutures.get(resource);
      if (configFuture == null) {
        throw new IllegalStateException("Failed to retrieve alter config future for resource " + resource + ".");
      }
      try {
        configFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        appliedConfigs.put(resource, configsByResource.get(resource));
      } catch (ExecutionException | TimeoutException e) {
        if (resource.type() == ConfigResource.Type.TOPIC && !topicExists(resource.name())) {
          LOG.debug("Failed to change configs for topic {} since it does not exist", resource.name());
          continue;
        }
        throw e;
      }
    }
    waitForConfigs(appliedConfigs);
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
   * It gets whether there is any throttled replica specified in a topic configuration property. If there is and the
   * specified throttled replica does not equal to "*", it modifies the configuration property by removing the
   * given set of replicas from the set of throttled replicas for each topic.
   *
   * @param throttledReplicasByTopic replicas to remove from the configuration properties of each topic.
   */
  private void removeThrottledReplicasFromTopics(Map<String, Set<String>> throttledReplicasByTopic)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (throttledReplicasByTopic.isEmpty()) {
      return;
    }

    Map<ConfigResource, Config> topicConfigsByResource = getEntityConfigsForTopics(throttledReplicasByTopic.keySet());
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : throttledReplicasByTopic.entrySet()) {
      String topic = entry.getKey();
      Set<String> replicas = entry.getValue();
      ConfigResource topicResource = topicConfigResource(topic);
      Config topicConfigs = topicConfigsByResource.getOrDefault(topicResource, EMPTY_CONFIG);
      List<AlterConfigOp> ops = new ArrayList<>();

      ConfigEntry currentLeaderThrottledReplicas = topicConfigs.get(LEADER_THROTTLED_REPLICAS);
      if (currentLeaderThrottledReplicas != null) {
        if (currentLeaderThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
          LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle");
        } else {
          replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} and replica {}", topic, r));
          String newThrottledReplicas = removeReplicasFromConfig(currentLeaderThrottledReplicas.value(), replicas);
          if (newThrottledReplicas.isEmpty()) {
            ops.add(new AlterConfigOp(new ConfigEntry(LEADER_THROTTLED_REPLICAS, null), AlterConfigOp.OpType.DELETE));
          } else {
            ops.add(new AlterConfigOp(new ConfigEntry(LEADER_THROTTLED_REPLICAS, newThrottledReplicas), AlterConfigOp.OpType.SET));
          }
        }
      }
      ConfigEntry currentFollowerThrottledReplicas = topicConfigs.get(FOLLOWER_THROTTLED_REPLICAS);
      if (currentFollowerThrottledReplicas != null) {
        if (currentFollowerThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
          LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle");
        } else {
          replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
          String newThrottledReplicas = removeReplicasFromConfig(currentFollowerThrottledReplicas.value(), replicas);
          if (newThrottledReplicas.isEmpty()) {
            ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_THROTTLED_REPLICAS, null), AlterConfigOp.OpType.DELETE));
          } else {
            ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_THROTTLED_REPLICAS, newThrottledReplicas), AlterConfigOp.OpType.SET));
          }
        }
      }
      if (!ops.isEmpty()) {
        configsToChange.put(topicResource, ops);
      }
    }
    changeConfigs(configsToChange);
  }

  private void removeThrottledRateFromBrokers(Set<Integer> brokerIds)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (brokerIds.isEmpty()) {
      return;
    }

    List<ConfigResource> brokerResources = brokerConfigResources(brokerIds);
    Map<ConfigResource, Config> brokerConfigsByResource = getEntityConfigs(brokerResources, false);
    Map<ConfigResource, Collection<AlterConfigOp>> configsToChange = new HashMap<>();
    for (ConfigResource brokerResource : brokerResources) {
      Config brokerConfigs = brokerConfigsByResource.get(brokerResource);
      ConfigEntry currLeaderThrottle = brokerConfigs.get(LEADER_THROTTLED_RATE);
      ConfigEntry currFollowerThrottle = brokerConfigs.get(FOLLOWER_THROTTLED_RATE);
      List<AlterConfigOp> ops = new ArrayList<>();
      if (currLeaderThrottle != null) {
        if (currLeaderThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)) {
          LOG.debug("Skipping removal for static leader throttle rate: {}", currFollowerThrottle);
        } else {
          LOG.debug("Removing leader throttle rate: {} on broker {}", currLeaderThrottle, brokerResource.name());
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_THROTTLED_RATE, null), AlterConfigOp.OpType.DELETE));
        }
      }
      if (currFollowerThrottle != null) {
        if (currFollowerThrottle.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)) {
          LOG.debug("Skipping removal for static follower throttle rate: {}", currFollowerThrottle);
        } else {
          LOG.debug("Removing follower throttle rate: {} on broker {}", currFollowerThrottle, brokerResource.name());
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_THROTTLED_RATE, null), AlterConfigOp.OpType.DELETE));
        }
      }
      if (!ops.isEmpty()) {
        configsToChange.put(brokerResource, ops);
      }
    }
    changeConfigs(configsToChange);
  }

  // Retries until we can read the configs changes we just wrote.
  void waitForConfigs(ConfigResource cf, Collection<AlterConfigOp> ops) {
    if (ops.isEmpty()) {
      return;
    }
    waitForConfigs(Collections.singletonMap(cf, ops));
  }

  void waitForConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configsByResource) {
    if (configsByResource.isEmpty()) {
      return;
    }

    Map<ConfigResource, Map<String, String>> expectedConfigsByResource = new HashMap<>();
    for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : configsByResource.entrySet()) {
      // Use HashMap::new instead of Collectors.toMap to allow inserting null values.
      Map<String, String> expectedConfigs = entry.getValue().stream()
          .collect(HashMap::new, (m, o) -> m.put(o.configEntry().name(), o.configEntry().value()), HashMap::putAll);
      expectedConfigsByResource.put(entry.getKey(), expectedConfigs);
    }

    long pollIntervalMs = INITIAL_CONFIG_CHANGE_CHECK_INTERVAL_MS;
    Exception lastException = null;
    List<ConfigResource> resources = orderedConfigResources(configsByResource.keySet());
    for (int attempt = 1; attempt <= _retries; attempt++) {
      try {
        Map<ConfigResource, Config> configs = getEntityConfigs(resources, true);
        boolean allConfigsApplied = true;
        for (Map.Entry<ConfigResource, Map<String, String>> entry : expectedConfigsByResource.entrySet()) {
          ConfigResource resource = entry.getKey();
          Config config = configs.get(resource);
          if (config == null && resource.type() == ConfigResource.Type.TOPIC) {
            continue;
          }
          if (config == null || !configsEqual(config, entry.getValue())) {
            allConfigsApplied = false;
            break;
          }
        }
        if (allConfigsApplied) {
          return;
        }
        lastException = null;
      } catch (ExecutionException | TimeoutException e) {
        lastException = e;
        LOG.debug("Failed to verify configs {} on attempt {}.", resources, attempt, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for configs " + configsByResource + " to be applied.", e);
      }

      if (attempt < _retries) {
        sleepForConfigChangeVisibility(pollIntervalMs);
        pollIntervalMs = Math.min(pollIntervalMs * 2, MAX_CONFIG_CHANGE_CHECK_INTERVAL_MS);
      }
    }

    throw new IllegalStateException("The following configs " + configsByResource + " were not applied within the time limit.", lastException);
  }

  private void sleepForConfigChangeVisibility(long pollIntervalMs) {
    try {
      Thread.sleep(pollIntervalMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for config changes to become visible.", e);
    }
  }

  private List<ConfigResource> brokerConfigResources(Collection<Integer> brokerIds) {
    return brokerIds.stream().sorted().map(ReplicationThrottleHelper::brokerConfigResource).collect(Collectors.toList());
  }

  private List<ConfigResource> topicConfigResources(Collection<String> topics) {
    return topics.stream().sorted().map(ReplicationThrottleHelper::topicConfigResource).collect(Collectors.toList());
  }

  private List<ConfigResource> orderedConfigResources(Collection<ConfigResource> resources) {
    List<ConfigResource> orderedResources = new ArrayList<>(resources);
    orderedResources.sort((resource1, resource2) -> {
      int typeCompare = resource1.type().compareTo(resource2.type());
      if (typeCompare != 0) {
        return typeCompare;
      }
      if (resource1.type() == ConfigResource.Type.BROKER) {
        return Integer.compare(Integer.parseInt(resource1.name()), Integer.parseInt(resource2.name()));
      }
      return resource1.name().compareTo(resource2.name());
    });
    return orderedResources;
  }

  private static ConfigResource brokerConfigResource(int brokerId) {
    return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
  }

  private static ConfigResource topicConfigResource(String topic) {
    return new ConfigResource(ConfigResource.Type.TOPIC, topic);
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry configEntry = configs.get(entry.getKey());
      if (configEntry == null || configEntry.value() == null || configEntry.value().isEmpty()) {
        if (entry.getValue() != null) {
          return false;
        }
      } else if (configEntry.source().equals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG) && entry.getValue() == null) {
        LOG.debug("Found static broker config: {}, skipping comparison", configEntry);
      } else if (!Objects.equals(entry.getValue(), configEntry.value())) {
        return false;
      }
    }
    return true;
  }

  private enum LogConfig {
    LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
    FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
  }

  /**
   * Starting with Kafka 3.5.0, the location of the "LogConfig" class is "org.apache.kafka.storage.internals.log.LogConfig"
   * and provides new constant fields in place of some methods of the original class.
   *
   *   - LogConfig class in Kafka 3.5+: org.apache.kafka.storage.internals.log.LogConfig
   *   - LogConfig class in Kafka 3.4-: kafka.log.LogConfig
   **
   * The older LogConfig class does not work with the newer versions of Kafka. Therefore, if the new class exists, we use it and if
   * it doesn't exist we will fall back on the older one.
   *
   * Once CC supports only 3.5.0 and newer, we can clean this up and use the LogConfig class from
   * `org.apache.kafka.storage.internals.log.LogConfig`all the time.
   * @param config LogConfig config name
   * @return LogConfig config name in the format of a Kafka configuration property
   */
  private static String getLogConfig(LogConfig config) {
    Class<?> logConfigClass;

    try {
      // First we try to get the LogConfig class for Kafka 3.5+
      logConfigClass = Class.forName(LOG_CONFIG_IN_KAFKA_3_5_AND_LATER);

      Field field = logConfigClass.getDeclaredField(config.toString());
      return field.get(null).toString();
    } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
      LOG.info("Failed to read config {} from LogConfig class since we are probably on kafka 3.4 or older: {}", config, e);
    }

    // We did not find the LogConfig class or field from Kafka 3.5+.
    // So we are probably on older Kafka version => we will try the older class for Kafka 3.4-.
    try {
      logConfigClass = Class.forName(LOG_CONFIG_IN_KAFKA_3_4_AND_EARLIER);

      String nameOfMethod = "";
      if (config == LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG) {
        nameOfMethod = "LeaderReplicationThrottledReplicasProp";
      } else if (config == LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG) {
        nameOfMethod = "FollowerReplicationThrottledReplicasProp";
      }

      Method method = logConfigClass.getMethod(nameOfMethod);
      return method.invoke(null).toString();
      } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      // No class or method was found for any Kafka version => we should fail
      throw new RuntimeException("Failed to read config " + config + " from LogConfig class:", e);
      }
    }
}
