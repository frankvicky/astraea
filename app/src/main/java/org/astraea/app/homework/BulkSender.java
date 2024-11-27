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
package org.astraea.app.homework;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.app.argument.DataSizeField;
import org.astraea.app.argument.StringListField;
import org.astraea.common.DataSize;
import org.astraea.common.admin.AdminConfigs;

public class BulkSender {
  private static final byte[] msgBody = new byte[1048576];

  public static void main(String[] args) throws IOException, InterruptedException {
    execute(Argument.parse(new Argument(), args));
  }

  public static void execute(final Argument param) throws IOException, InterruptedException {
    // you must create topics for best configs
    try (var admin =
        Admin.create(Map.of(AdminConfigs.BOOTSTRAP_SERVERS_CONFIG, param.bootstrapServers()))) {
      for (var t : param.topics) {
        admin.createTopics(List.of(new NewTopic(t, 1, (short) 1))).all();
      }
    }
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, param.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    // you must manage producers for best performance
    try (var producer = new KafkaProducer<>(props)) {
      var size = new AtomicLong(0);
      var key = "key";
      var value = "value";
      var num = 0;
      while (size.get() < param.dataSize.bytes()) {
        var topic = param.topics.get(num++ % param.topics.size());
        producer.send(
            new ProducerRecord<>(topic, key, value),
            (m, e) -> {
              if (e == null) size.addAndGet(m.serializedKeySize() + m.serializedValueSize());
            });
      }
    }
  }

  public static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names which you should send",
        validateWith = StringListField.class,
        listConverter = StringListField.class,
        required = true)
    List<String> topics;

    @Parameter(
        names = {"--dataSize"},
        description = "data size: total size you have to send",
        validateWith = DataSizeField.class,
        converter = DataSizeField.class)
    DataSize dataSize = DataSize.GB.of(10);
  }
}
