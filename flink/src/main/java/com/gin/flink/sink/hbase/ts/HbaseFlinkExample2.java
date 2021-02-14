package com.gin.flink.sink.hbase.ts;


import com.gin.flink.sink.hbase.HBaseReaderSource;
import com.gin.flink.sink.hbase.HBaseWriterSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;


/**
 *
 * hbase shell 中创建表并插入数据, 最终 pns 的数据插入 psn2
create 'psn' , 'cf'
put 'psn', '1', 'cf:name', 'gin'
put 'psn', '2', 'cf:name', 'soul'
put 'psn', '11', 'cf:name', 'gin_soul'
create 'psn2' , 'cf'
scan 'psn'
scan 'psn2'
 *
 */
public class HbaseFlinkExample2 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 参数设置
        env.getConfig().setGlobalJobParameters(null);

        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("HBase Reader add source");
        DataStreamSource stream = env.createInput(new HBaseInputFormat());
        stream.print().setParallelism(1);
        // NO.1 Sink
        System.out.println("HBase Reader add sink");
        stream.writeUsingOutputFormat(new HBaseOutputFormat());
        // NO.2 Output
        //System.out.println("HBase Reader output format");
        //stream.writeUsingOutputFormat(new HBaseOutputFormat());
        env.execute();
    }
}