package com.ocean.flink.function.executionenvironment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * event time 需要指定event time类型并指定水位线；process time无需指定
         * 水位线与事件时间一起使用，用来处理乱序事件
         * 每一条数据都有自己的水位线，水位线的时间小于当前数据的事件时间，代表着小于当前水位线时间的数据都已到达
         * 当水位线的时间戳大于等于窗口结束时间时，意味着窗口结束，触发窗口计算
         * 水位线的计算方式为：进入窗口的最大事件时间 - 指定的延迟时间
         * */

        // 设置缓冲区的最大等待时间，最大等待时间为 -1时，表示删除超时，直到缓冲区已满时才会刷新
        env.setBufferTimeout(1000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("192.168.91.1", 9999)
                .flatMap(new map())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();
        env.execute("word count");

    }

    private static class map implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : value.split(",")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
