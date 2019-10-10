/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSink;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.selector.DefaultTopicSelector;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueSerializationSchema;

public class RocketMQFlinkExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(300000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "consumer-group-sku");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-source");

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        producerProps.setProperty(RocketMQConfig.PRODUCER_RETRY_TIMES,"10");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps))
            .name("rocketmq-source")
            .setParallelism(4)
            .process(new ProcessFunction<Map, Map>() {
                @Override
                public void processElement(Map in, Context ctx, Collector<Map> out) throws Exception {
                    System.err.println(in.get("id"));
                    System.err.println(in.get("address"));
                    HashMap result = new HashMap();
                    result.put("id", in.get("id"));
                    String[] arr = in.get("address").toString().split("\\s+");
                    result.put("province", arr[arr.length-1]);
                    out.collect(result);
                }
            })
            .name("upper-processor")
            .setParallelism(4)





            .addSink(new RocketMQSink(new SimpleKeyValueSerializationSchema("id", "province"),
                new DefaultTopicSelector("flink-sink2"), producerProps).withBatchFlushOnCheckpoint(true))
            .name("rocketmq-sink")
            .setParallelism(4);
        try {
            env.execute("rocketmq-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
