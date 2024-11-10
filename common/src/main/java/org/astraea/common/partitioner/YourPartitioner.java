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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class YourPartitioner implements Partitioner {
  // Broker ID 到空間利用率的映射
  private final Map<Integer, Long> brokerSpaceUsage = new ConcurrentHashMap<>();

  // Partition 到 Broker ID 的映射
  private final Map<Integer, Integer> partitionToBroker = new ConcurrentHashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public int partition(
      String topic,
      Object keyObj,
      byte[] keyBytes,
      Object valueObj,
      byte[] valueBytes,
      Cluster cluster) {

    // 獲取 topic 的所有分區信息
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

    // 更新 partitionToBroker 映射
    for (PartitionInfo partitionInfo : partitions) {
      int partition = partitionInfo.partition();
      int brokerId = partitionInfo.leader().id();
      partitionToBroker.put(partition, brokerId);

      // 初始化 brokerSpaceUsage，如果還沒有該 broker 的記錄
      brokerSpaceUsage.putIfAbsent(brokerId, 0L);
    }

    // 計算消息的大小
    long messageSize = valueBytes != null ? valueBytes.length : 0;

    // 找到空間利用率最低的 broker
    int selectedBroker =
        brokerSpaceUsage.entrySet().stream()
            .min(Comparator.comparingLong(Map.Entry::getValue))
            .get()
            .getKey();

    // 在 selectedBroker 上找到可用的分區
    List<Integer> candidatePartitions = new ArrayList<>();
    for (Map.Entry<Integer, Integer> entry : partitionToBroker.entrySet()) {
      if (entry.getValue() == selectedBroker) {
        candidatePartitions.add(entry.getKey());
      }
    }

    // 如果沒有找到，則使用默認的輪詢方式
    int selectedPartition;
    if (candidatePartitions.isEmpty()) {
      // 使用輪詢方式選擇分區
      selectedPartition = Math.abs(Arrays.hashCode(keyBytes)) % partitions.size();
    } else {
      // 從候選分區中選擇一個
      selectedPartition = candidatePartitions.get(new Random().nextInt(candidatePartitions.size()));
    }

    // 更新選定 broker 的空間利用率
    brokerSpaceUsage.compute(selectedBroker, (k, v) -> v + messageSize);

    return selectedPartition;
  }

  @Override
  public void close() {}
}
