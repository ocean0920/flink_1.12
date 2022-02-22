package com.ocean.flink.function.timeandwatermark;


import com.ocean.flink.pojo.SensorReading;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * flink支持事件时间、处理时间、提取时间
 * 如果要使用事件时间，需给flink提供时间戳提取器和watermark生成器，flink使用它们跟踪事件时间的进度
 * 如果不使用根据key进行分组的键控时间流，就不能并行处理
 * 超过最大无序边界的时间会被删除，或者收集到侧输出流中
 * */
public class EventTimeAndWaterMark {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> stream = env.fromElements(new SensorReading("key", 1L, 1));

        // 侧输出流，用于收集超过最大无序边界的事件
        OutputTag<SensorReading> outputTag = new OutputTag<>("late_data");
        // ProcesWindowFunction 实例
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> event = stream
                .keyBy(x -> x.key)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sideOutputLateData(outputTag) // 输出到侧输出流
                .allowedLateness(Time.seconds(10)) // 允许延迟10秒，即超过watermark10秒
                .process(new MyWasterfulMax());

        DataStream<SensorReading> lateStream = event.getSideOutput(outputTag);
    }
}
