package com.test.session;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<MyData> text = env.addSource(new MySource()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MyData>() {
            @Override
            public long extractAscendingTimestamp(MyData myData) {
                return myData.getTime();
            }
        }).setParallelism(1);

        SingleOutputStreamOperator<MyData> sum = text.keyBy(new KeySelector<MyData, Integer>() {
            @Override
            public Integer getKey(MyData myData) throws Exception {
                return myData.getKey();
            }
        }).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .trigger(MyTrigger.of(Time.seconds(3)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(10))
                .process(new ProcessWindowFunction<MyData, MyData, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<MyData> iterable, Collector<MyData> collector) throws Exception {
                        for(MyData i : iterable){
                            collector.collect(i);
                        }
                    }
                });

        sum.addSink(new MySink());

        env.execute("Session");
    }
}
