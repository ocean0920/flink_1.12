package com.ocean.flink.function.statetransition;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 针对一对流
 * */
public class RichCoFlatMapFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<String, String> control = env.fromElements("spark", "flink")
                .keyBy(x -> x);

        KeyedStream<String, String> streamOfWords = env.fromElements("hive", "hadoop", "spark", "flink")
                .keyBy(x -> x);

        control.connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    // 当有一对数据流的时候，使用RichCoFlatMapFunction
    public static class ControlFunction extends RichCoFlatMapFunction<String,String,String>{

        private ValueState<Boolean> blockend;

        @Override
        public void open(Configuration config) throws Exception {
            blockend = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Boolean>("blocked",Boolean.class));
        }

        /**
         * 在flink运行时，flatMap1和flatMap2在连接流有新元素到来时被调用 --在例子中，control流中的元素会进入flatMap1，
         * streamOfWords中的元素会进入flatMap2.这是由两个流连接的顺序决定的，本例中为control.connect(streamOfWords)
         * */
        @Override
        public void flatMap1(String value, Collector<String> out) throws Exception {

            blockend.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String value, Collector<String> out) throws Exception {

            if(blockend.value() == null){
                out.collect(value);
            }
        }
    }
}
