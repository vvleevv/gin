package com.gin.flink.common.unrepeated;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gin
 * @date 2021
 */
public class UnrepeatedKeyProcess
        extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, String>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(UnrepeatedKeyProcess.class);

    private ValueState<Boolean> existState;

    @Override
    public void open(Configuration parameters) {
        //1天过期
        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                //过期不可访问
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                //在状态值被创建和被更新时重设TTL
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 在每处理10000条状态记录之后，更新检测过期的时间戳
                // 更新太频繁会降低compaction的性能
                // 更新过慢会使得compaction不及时，状态空间膨胀
                .cleanupInRocksdbCompactFilter(10000)
                .build();

        ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<>(
                "unrepeated-order-state",
                Boolean.class
        );
        existStateDesc.enableTimeToLive(stateTtlConfig);

        existState = this.getRuntimeContext().getState(existStateDesc);
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
        //不存在的key才继续收集
        if (existState.value() == null) {
            //更新为存在
            existState.update(true);
            out.collect(value);
        }
    }
}