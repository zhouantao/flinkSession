package com.test.session;

import java.text.SimpleDateFormat;

public class DataUtils {
    public static String longToString(long time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String string = sdf.format(time);
        return string;
    }
}
