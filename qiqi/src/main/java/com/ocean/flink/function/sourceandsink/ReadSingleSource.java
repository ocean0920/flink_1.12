package com.ocean.flink.function.sourceandsink;


import com.ocean.flink.pojo.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

// flink将Tuples and Pojos类型序列化成流
public class ReadSingleSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Person> people = new ArrayList<>();
        people.add(new Person("kafka",1));
        people.add(new Person("spark",2));
        people.add(new Person("flink",3));

        DataStreamSource<Person> source = env.fromCollection(people);
        SingleOutputStreamOperator<Person> result = source.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person value) throws Exception {
                return value.age > 2;
            }
        });

        result.print();
        env.execute();

        //flink读取简单数据源的方式：fromElements  fromCollection  socketTextStream  readTextFile
//        DataStreamSource<Person> source = env.fromElements(
//                new Person("kafka", 1),
//                new Person("spark", 2),
//                new Person("flink", 3)
//        );
//        env.readTextFile("")
    }
}
