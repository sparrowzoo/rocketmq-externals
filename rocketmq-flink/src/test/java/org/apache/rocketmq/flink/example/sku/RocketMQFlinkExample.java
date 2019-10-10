/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.example.sku;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.serialization.SimpleKeyValueDeserializationSchema;

import java.util.Map;
import java.util.Properties;

public class RocketMQFlinkExample {
    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            //-s hdfs:///flink/checkpoints/d32f0b85e75e4b5c12500b1041938955/chk-533
            args = new String[] {"-s", "file:///d:/workspace/flink-checkpoint/14420a7321e90c695a0e001bb727d288/chk-7"};
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);

        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.setStateBackend(new FsStateBackend("file:///d:/workspace/flink-checkpoint"));

env.setRestartStrategy(RestartStrategies.fallBackRestart());
// advanced options:

// set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);


// allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
// enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c002");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "flink-sku");

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        producerProps.setProperty(RocketMQConfig.PRODUCER_RETRY_TIMES, "10");

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema(), consumerProps))
                .name("rocketmq-source")
                .setParallelism(4)
                .process(new ProcessFunction<Map, SkuOrderEvent>() {
                    @Override
                    public void processElement(Map in, Context ctx, Collector<SkuOrderEvent> out) throws Exception {
                        String value = (String) in.get("value");
                        SkuOrderEvent skuOrderEvent = JSON.parseObject(value, SkuOrderEvent.class);
                        out.collect(skuOrderEvent);
                    }
                })
                .name("upper-processor")
                .setParallelism(4)
                .flatMap(new FlatMapFunction<SkuOrderEvent, UserSkuCount>() {
                    @Override
                    public void flatMap(SkuOrderEvent o, Collector<UserSkuCount> collector) throws Exception {
                        collector.collect(new UserSkuCount(o.getUserId()));
                    }
                }).name("user-sku-map").setParallelism(4)
                .keyBy("userId")
                .timeWindow(Time.hours(1),Time.milliseconds(200))

                .reduce(new ReduceFunction<UserSkuCount>() {
                    @Override
                    public UserSkuCount reduce(UserSkuCount o, UserSkuCount t1) throws Exception {
                        UserSkuCount userSkuCount = new UserSkuCount(o.getUserId(), o.getCount() + t1.getCount());
                        //System.out.println(userSkuCount);
                        return userSkuCount;
                    }
                }).name("user-sku-sum-reduce").setParallelism(4)
                .windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(200)))
                .process(new TopNFunction(5))
                .writeAsText("d://flink-sku").setParallelism(1);
//               .addSink(new SinkFunction() {
//                   @Override
//                   public void invoke(Object value, Context context) throws Exception {
//                       System.out.println(value);
//                       System.out.println(context);
//                   }
//               });
//            .addSink(new RocketMQSink(new SimpleKeyValueSerializationSchema(),
//                new DefaultTopicSelector("flink-sku"), producerProps).withBatchFlushOnCheckpoint(true))
//            .name("rocketmq-sink")
//            .setParallelism(4);
        try {
            env.execute("rocketmq-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


