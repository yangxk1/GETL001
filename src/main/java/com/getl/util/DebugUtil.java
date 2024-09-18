package com.getl.util;

import groovy.util.logging.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class DebugUtil {

    private static final Logger log = LoggerFactory.getLogger(DebugUtil.class);

    public static void DebugInfo(String info) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("### ");
        stringBuilder.append(info);
        stringBuilder.append("\n===========current state===========");
        stringBuilder.append("\ntime: ").append(System.currentTimeMillis());
        long max = Runtime.getRuntime().maxMemory();
        stringBuilder.append("\nJVM max Memory (Byte): ").append(max);
        long totalMemory = Runtime.getRuntime().totalMemory();
        stringBuilder.append("\nJVM current total Memory (Byte): ").append(totalMemory);
        long free = Runtime.getRuntime().freeMemory();
        stringBuilder.append("\nJVM current free Memory (Byte): ").append(free);
        long used = totalMemory - free;
        stringBuilder.append("\nUsed Memory (Byte): ").append(used);
        stringBuilder.append("\n====================================");
        log.info(stringBuilder.toString());
    }
}
