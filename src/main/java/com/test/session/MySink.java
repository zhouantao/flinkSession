package com.test.session;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import static com.test.session.DataUtils.longToString;

public class MySink implements SinkFunction<MyData> {
    @Override
    public void invoke(MyData value, Context context) throws Exception {
        System.out.println(value + "  sink处理时间" + longToString(System.currentTimeMillis()));
    }
}
