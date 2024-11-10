/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.partitioner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {

  private final Set<Integer> nodes = new HashSet<>();
  private final PriorityQueue<BrokerUsage> brokerUsageQueue =
      new PriorityQueue<>(Comparator.comparingLong(b -> b.usage));
  private final Map<Integer, List<PartitionInfo>> brokerToPartitions = new HashMap<>();
  private Iterator<Integer> roundRobinIterator; // 用於輪詢初始化

  private long maxUsageThreshold = 2L * 1024 * 1024 * 1024; // 初始負載門檻，2 GB

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

    if (brokerUsageQueue.isEmpty() || nodes.size() != cluster.nodes().size()) {
      initializeBrokerUsage(cluster);
      assignPartitionsToBrokers(cluster.availablePartitionsForTopic(topic));
      initializeRoundRobinIterator();
    }

    return selectPartitionForBroker(valueBytes);
  }

  private void initializeRoundRobinIterator() {
    roundRobinIterator = nodes.iterator();
  }

  private void assignPartitionsToBrokers(List<PartitionInfo> partitions) {
    brokerToPartitions.values().forEach(List::clear);

    for (PartitionInfo partitionInfo : partitions) {
      int brokerId = getNextBrokerInRoundRobin();
      brokerToPartitions.get(brokerId).add(partitionInfo);
    }
  }

  private int getNextBrokerInRoundRobin() {
    if (!roundRobinIterator.hasNext()) {
      initializeRoundRobinIterator();
    }
    return roundRobinIterator.next();
  }

  private int selectPartitionForBroker(byte[] valueBytes) {
    PriorityQueue<BrokerUsage> usageQueueCopy = new PriorityQueue<>(brokerUsageQueue);
    long dataSize = calculateDataSize(valueBytes);

    while (!usageQueueCopy.isEmpty()) {
      BrokerUsage leastLoadedBroker = usageQueueCopy.poll();
      int brokerId = leastLoadedBroker.brokerId;
      List<PartitionInfo> assignedPartitions = brokerToPartitions.get(brokerId);

      if (!assignedPartitions.isEmpty() && leastLoadedBroker.usage + dataSize < maxUsageThreshold) {
        PartitionInfo selectedPartition = assignedPartitions.remove(0);
        updateBrokerUsage(brokerId, dataSize);
        return selectedPartition.partition();
      }
    }

    adjustThreshold(); // 動態調整門檻
    return getFallbackPartition(); // 使用回退分區
  }

  private int getFallbackPartition() {
    for (List<PartitionInfo> partitions : brokerToPartitions.values()) {
      if (!partitions.isEmpty()) {
        return partitions.get(0).partition();
      }
    }
    return 0;
  }

  private void updateBrokerUsage(int brokerId, long dataSize) {
    brokerUsageQueue.removeIf(b -> b.brokerId == brokerId);
    brokerUsageQueue.add(new BrokerUsage(brokerId, dataSize));
  }

  private void initializeBrokerUsage(Cluster cluster) {
    brokerUsageQueue.clear();
    nodes.clear();
    brokerToPartitions.clear();
    for (Node node : cluster.nodes()) {
      brokerUsageQueue.add(new BrokerUsage(node.id(), 0L));
      brokerToPartitions.put(node.id(), new ArrayList<>());
      nodes.add(node.id());
    }
  }

  private long calculateDataSize(byte[] data) {
    return data == null ? 0L : data.length;
  }

  private void adjustThreshold() {
    BrokerUsage maxLoadBroker = brokerUsageQueue.peek();
    if (maxLoadBroker != null) {
      if (maxLoadBroker.usage > maxUsageThreshold * 0.8) {
        maxUsageThreshold += 256 * 1024 * 1024; // 增加 256 MB
      } else if (maxLoadBroker.usage < maxUsageThreshold * 0.5) {
        maxUsageThreshold =
            Math.max(2L * 1024 * 1024 * 1024, maxUsageThreshold - 256 * 1024 * 1024); // 降低並設下限
      }
    }
  }

  private static class BrokerUsage {
    int brokerId;
    long usage;

    BrokerUsage(int brokerId, long usage) {
      this.brokerId = brokerId;
      this.usage = usage;
    }
  }

  @Override
  public void close() {}
}
