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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class YourPartitioner implements Partitioner {
  private final ConcurrentHashMap<Integer, Long> partitionLoad = new ConcurrentHashMap<>();
  private final Map<String, Long> brokerLoad = new ConcurrentHashMap<>();
  private final Map<String, Integer> brokerLeaderCount = new ConcurrentHashMap<>();
  private final Random random = new Random();
  private int previousPartition = -1;

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    int numPartitions = partitions.size();

    if (numPartitions != partitionLoad.size()) {
      partitions.forEach(p -> partitionLoad.putIfAbsent(p.partition(), 0L));
      updateBrokerLoad(partitions, cluster);
      updateBrokerLeaderCount(partitions);
    }

    int targetPartition = selectPartition(partitions);
    long messageSize = valueBytes == null ? 0 : valueBytes.length;

    // 更新目標分區負載
    partitionLoad.merge(targetPartition, messageSize, Long::sum);

    // 更新 broker 負載和 leader 計數
    String leaderBrokerId =
        cluster.leaderFor(new TopicPartition(topic, targetPartition)).idString();
    brokerLoad.merge(leaderBrokerId, messageSize, Long::sum);
    brokerLeaderCount.merge(leaderBrokerId, 1, Integer::sum);

    return targetPartition;
  }

  private void updateBrokerLoad(List<PartitionInfo> partitions, Cluster cluster) {
    brokerLoad.clear();
    partitions.forEach(
        partition -> {
          long partitionSize = partitionLoad.getOrDefault(partition.partition(), 0L);
          for (Node replica : partition.replicas()) {
            String brokerId = cluster.nodeById(replica.id()).idString();
            brokerLoad.merge(brokerId, partitionSize, Long::sum);
          }
        });
  }

  private void updateBrokerLeaderCount(List<PartitionInfo> partitions) {
    brokerLeaderCount.clear();
    partitions.forEach(
        partition -> {
          String leaderId = partition.leader().idString();
          brokerLeaderCount.merge(leaderId, 1, Integer::sum);
        });
  }

  private double calculateAAD() {
    double mean = partitionLoad.values().stream().mapToLong(Long::longValue).average().orElse(0.0);
    return partitionLoad.values().stream()
        .mapToDouble(load -> Math.abs(load - mean))
        .average()
        .orElse(0.0);
  }

  private int selectPartition(List<PartitionInfo> partitions) {
    double aad = calculateAAD();

    return partitions.stream()
        .filter(p -> partitionLoad.getOrDefault(p.partition(), 0L) < aad)
        .filter(
            p -> {
              String leaderId = p.leader().idString();
              return brokerLoad.getOrDefault(leaderId, 0L) < getBrokerLoadThreshold()
                  && brokerLeaderCount.getOrDefault(leaderId, 0) < getMaxLeaderCount();
            })
        .findFirst()
        .map(PartitionInfo::partition)
        .orElseGet(this::getRandomPartition);
  }

  private long getBrokerLoadThreshold() {
    // 設定 broker 的負載門檻，例如設定為 2 GB
    return 2L * 1024 * 1024 * 1024; // 2 GB
  }

  private int getMaxLeaderCount() {
    // 設定單一 broker 最大的 leader 分區數
    return 2; // 例如每個 broker 最多有 2 個 leader
  }

  private int getRandomPartition() {
    int newPartition;
    do {
      newPartition = random.nextInt(partitionLoad.size());
    } while (newPartition == previousPartition);
    previousPartition = newPartition;
    return newPartition;
  }

  @Override
  public void close() {}
}
