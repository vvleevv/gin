/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gin.flink.demo;

import com.gin.flink.sink.hbase.batch.HBaseOutputFormat;
import com.gin.flink.sink.hbase.stream.HBaseWriterSink;
import com.gin.flink.sink.hbase.stream.HBaseWriterSinkSingle;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSink;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.serialization.SimpleTupleDeserializationSchema;
import org.apache.rocketmq.flink.function.SinkMapFunction;
import org.apache.rocketmq.flink.function.SourceMapFunction;

import java.util.Properties;

import static org.apache.rocketmq.flink.RocketMQConfig.CONSUMER_OFFSET_LATEST;
import static org.apache.rocketmq.flink.RocketMQConfig.DEFAULT_CONSUMER_TAG;

public class RocketMQToHbase {

    /**
     * Source Config
     * @return properties
     */
    private static Properties getConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
                "10.0.0.21:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "GroupTest");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "SOURCE_TOPIC");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TAG, DEFAULT_CONSUMER_TAG);
        consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
        consumerProps.setProperty(RocketMQConfig.ACCESS_CHANNEL, AccessChannel.CLOUD.name());
        return consumerProps;
    }

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // for local
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // for cluster
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        env.setStateBackend(new MemoryStateBackend());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start a checkpoint every 10s
        env.enableCheckpointing(10000);
        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties consumerProps = getConsumerProps();

        SimpleTupleDeserializationSchema schema = new SimpleTupleDeserializationSchema();

        DataStreamSource<Tuple2<String, String>> source = env.addSource(
                new RocketMQSource<>(schema, consumerProps)).setParallelism(2);

        source.print();
        System.out.println("HBase Reader add sink");
        source.addSink(new HBaseWriterSinkSingle());

        env.execute("rocketmq-to-hbase");
    }
}
