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

package org.apache.rocketmq.flink.example.sku;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Random;

public class ProducerTest {
    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("sku");
        producer.setNamesrvAddr("localhost:9876");
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            SkuOrderEvent skuOrderEvent=new SkuOrderEvent();
            skuOrderEvent.setCityId(1);
            skuOrderEvent.setCategoryId(1);
            skuOrderEvent.setSkuId(1);
            skuOrderEvent.setUserId(new Random().nextInt(10) +"");
            skuOrderEvent.setTimestamp(System.currentTimeMillis());
            Message msg = new Message("flink-sku" ,
                    "", "id_"+i,
                    JSON.toJSONBytes(skuOrderEvent));
            try {
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("send " + i);
//            try {
//                if(i%10==0) {
//                    Thread.sleep(1000);
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }
}
