package com.getl.util;

import cn.hutool.core.io.FileUtil;
import com.getl.constant.CommonConstant;
import lombok.Getter;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class GetlLogger {
    @Getter
    private String logfileName = "debug.md";

    public GetlLogger(String logfileName) {
        this.logfileName = logfileName + ".md";
        FileUtil.appendUtf8String("# new " + logfileName + " log\n", CommonConstant.LOG_FILE_PATH + this.logfileName);
    }

    public GetlLogger() {
        FileUtil.appendUtf8String("# new " + logfileName + " log\n", CommonConstant.LOG_FILE_PATH + this.logfileName);
    }

    public void debugInfo(String info) {
        this.debugInfo(info, 0);
    }

    public void debugInfo(String info, long time) {
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        simpleDateFormat.setTimeZone(java.util.TimeZone.getTimeZone("GMT+8"));
        String formattedDate = simpleDateFormat.format(date);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\n## ");
        stringBuilder.append(info);
        stringBuilder.append("\n```");
        stringBuilder.append("\ncurrent time: ").append(formattedDate);
        stringBuilder.append("\nused time: ").append(NumberFormat.getInstance(Locale.US).format(time)).append(" ms");
        long max = Runtime.getRuntime().maxMemory();
        stringBuilder.append("\nJVM max Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(max)).append(" B");
        long totalMemory = Runtime.getRuntime().totalMemory();
        stringBuilder.append("\nJVM current total Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(totalMemory)).append(" B");
        long free = Runtime.getRuntime().freeMemory();
        stringBuilder.append("\nJVM current free Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(free)).append(" B");
        long used = totalMemory - free;
        stringBuilder.append("\nUsed Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(used)).append(" B");
        stringBuilder.append("\n```");
        System.out.println(stringBuilder.toString());
        FileUtil.appendUtf8String(stringBuilder.toString() + "\n\n", CommonConstant.LOG_FILE_PATH + this.logfileName);
    }
}
