package com.test.session;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<MyData> text = env.addSource(new MySource()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MyData>() {
            @Override
            public long extractAscendingTimestamp(MyData myData) {
                return myData.getTime();
            }
        });

        SingleOutputStreamOperator<MyData> sum = text.keyBy(new KeySelector<MyData, Integer>() {
            @Override
            public Integer getKey(MyData myData) throws Exception {
                return myData.getKey();
            }
        }).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .trigger(MyTrigger.of(Time.seconds(3)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(10))
                .sum("index");

        sum.addSink(new MySink());

        env.execute("Session");
    }
}
