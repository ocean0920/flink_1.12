package com.ocean.flink.function.timeandwatermark;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**使用watermarkStrategy生成水位线*/

public class WatermarkStrategyDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputSource = env.socketTextStream("192.168.91.1", 9999);
        // 定义从数据源提取时间戳的规则
        SerializableTimestampAssigner<String> timestampAssigner = new SerializableTimestampAssigner<String>() {

            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                Long aLong = new Long(fields[0]);
                return aLong * 1000L;
            }
        };
        // 生成水位线
        SingleOutputStreamOperator<String> watermarkStream = inputSource.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 定义延迟时间
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        // 传入从事件中抽取的事件戳
                        .withTimestampAssigner(timestampAssigner)
                        // 处理空闲数据流，如果超过一分钟，就将此数据流标记为空闲，意味着下游数据不需要等待此流的水位线
                        .withIdleness(Duration.ofMinutes(1)));

        //
        watermarkStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new Tuple2<>(fields[1],1));

            }
        }).keyBy(date -> date.f0).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum(1).print();

        env.execute("run watermark word count");
    }
}
