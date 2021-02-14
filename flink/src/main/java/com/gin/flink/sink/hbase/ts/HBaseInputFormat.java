package com.gin.flink.sink.hbase.ts;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

public class HBaseInputFormat extends CustomTableInputFormat {
    @Override
    protected Scan getScanner() {
        return null;
    }

    @Override
    protected String getTableName() {
        return null;
    }

    @Override
    protected Tuple mapResultToTuple(Result r) {
        return null;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {

    }
}
