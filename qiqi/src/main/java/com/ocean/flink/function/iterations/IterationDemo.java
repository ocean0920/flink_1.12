package com.ocean.flink.function.iterations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterationDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个序列流
        DataStreamSource<Long> someIntegers = env.fromSequence(1, 1000);
        // 将序列流放入迭代器
        IterativeStream<Long> iteration = someIntegers.iterate();
        // 指定在迭代循环内执行的逻辑，每个值减去1
        SingleOutputStreamOperator<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });
        // 对循环内的每一个结果进行过滤，持续生成大于0的值
        SingleOutputStreamOperator<Long> stillGenerateThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        // 将大于0的值重新放回迭代头进行重新部署迭代
        iteration.closeWith(stillGenerateThanZero);
        // 上述一直循环迭代，直到每个元素等于0，传递下游（最终输出1000个0）
        SingleOutputStreamOperator<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });
        // 打印控制台
        lessThanZero.print();

        env.execute();
    }
}
