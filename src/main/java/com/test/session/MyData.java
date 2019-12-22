package com.test.session;

import static com.test.session.DataUtils.longToString;

public class MyData {
    private int key = 0;
    private int index = 0;
    private long time = 0;

    public MyData() {
    }

    public MyData(int key, int index, long time) {
        this.key = key;
        this.index = index;
        this.time = time;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "MyData{" +
                "key=" + key +
                ", index=" + index +
                ", time=" + longToString(time) +
                '}';
    }

}
