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
  private final Random random = new Random();
  private int previousPartition = -1;

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
    int numPartitions = partitions.size();

    // 只在分區數量有變化時更新 broker 負載
    if (numPartitions != partitionLoad.size()) {
      partitions.forEach(p -> partitionLoad.putIfAbsent(p.partition(), 0L));
      updateBrokerLoad(partitions, cluster);
    }

    int targetPartition = selectPartition(partitions, cluster);
    long messageSize = valueBytes == null ? 0 : valueBytes.length;

    // 只更新實際寫入的分區負載
    partitionLoad.merge(targetPartition, messageSize, Long::sum);

    // 更新該訊息對應 broker 的負載
    String leaderBrokerId =
        cluster.leaderFor(new TopicPartition(topic, targetPartition)).idString();
    brokerLoad.merge(leaderBrokerId, messageSize, Long::sum);

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

  private double calculateAAD() {
    double mean = partitionLoad.values().stream().mapToLong(Long::longValue).average().orElse(0.0);
    return partitionLoad.values().stream()
        .mapToDouble(load -> Math.abs(load - mean))
        .average()
        .orElse(0.0);
  }

  private int selectPartition(List<PartitionInfo> partitions, Cluster cluster) {
    double aad = calculateAAD();

    return partitions.stream()
        .filter(p -> partitionLoad.getOrDefault(p.partition(), 0L) < aad)
        .filter(
            p -> {
              for (Node replica : p.replicas()) {
                String brokerId = cluster.nodeById(replica.id()).idString();
                if (brokerLoad.getOrDefault(brokerId, 0L) >= getBrokerLoadThreshold()) {
                  return false;
                }
              }
              return true;
            })
        .findFirst()
        .map(PartitionInfo::partition)
        .orElseGet(this::getRandomPartition);
  }

  private long getBrokerLoadThreshold() {
    // 設定 broker 的負載門檻，例如設定為 2 GB
    return 2L * 1024 * 1024 * 1024; // 2 GB
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
