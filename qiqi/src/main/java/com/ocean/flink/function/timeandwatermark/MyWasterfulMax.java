package com.ocean.flink.function.timeandwatermark;

import com.ocean.flink.pojo.SensorReading;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyWasterfulMax extends ProcessWindowFunction<SensorReading, //输入类型
        Tuple3<String,Long,Integer>, //输出类型
        String, //键类型
        TimeWindow> { //窗口类型

    @Override
    public void process(String key, Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {

        int max = 0;
        for (SensorReading element : elements) {
            max = Math.max(element.max_value, 0);
        }
        out.collect(Tuple3.of(key,context.window().getEnd(),max));

    }

}
