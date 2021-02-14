package com.gin.flink.sink.hbase.ts;
 
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseOutputFormat implements OutputFormat<Tuple2<String, String>> {

    //配置
    org.apache.hadoop.conf.Configuration config = null;
    //表管理
    Admin admin = null;
    private Connection conn = null;
    TableName tableName = null;
    private Table table = null;
 
    @Override
    public void configure(Configuration parameters) {
    }
 
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        System.out.println("Read source open");
        //创建配置文件对象
        config = HBaseConfiguration.create();
        //加载ZK配置
        config.set("hbase.zookeeper.quorum", "node02,node03,node04");
        conn = ConnectionFactory.createConnection(config);
        //获取数据操作对象
        tableName = TableName.valueOf("psn");
        table = conn.getTable(tableName);

    }
 
    @Override
    public void writeRecord(org.apache.flink.api.java.tuple.Tuple2<String, String> record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.f0));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("test1"), Bytes.toBytes(record.f1));
        table.put(put);
    }
 
    @Override
    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}