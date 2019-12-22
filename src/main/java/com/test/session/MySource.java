package com.test.session;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;

public class MySource implements SourceFunction<MyData> {
    private Boolean isRunning = true;
    @Override
    public void run(SourceContext<MyData> sourceContext) throws Exception {
        int key = 0;
        int index = 0;
        int count = 0;
        while(isRunning){
            long time = System.currentTimeMillis();
            MyData myData = new MyData(key,index,time);
            System.out.println("发送一条当前数据:" + myData);
            count++;
            index++;
            sourceContext.collect(myData);
            if(count == 1){
                Thread.sleep(1000);
            }else if(count == 2){
                Thread.sleep(7000);
            }else if(count >= 3){
                isRunning = false;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
