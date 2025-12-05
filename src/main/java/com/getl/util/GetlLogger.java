package com.getl.util;

import cn.hutool.core.io.FileUtil;
import com.getl.constant.CommonConstant;
import lombok.Getter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class GetlLogger {
    @Getter
    private String logfileName = CommonConstant.LOG_FILE_PATH + "debug.md";
    private String summaryFileName = CommonConstant.LOG_FILE_PATH + "summary/" + "summary.csv";

    // Cache for storing time and memory consumption for each info type
    private final Map<String, List<Long>> infoTimeCache = new HashMap<>();
    private final Map<String, List<Long>> infoMemoryCache = new HashMap<>();

    public GetlLogger(String logfileName) {
        this.logfileName = CommonConstant.LOG_FILE_PATH + logfileName + ".md";
        this.summaryFileName = CommonConstant.LOG_FILE_PATH + "summary/" + logfileName + ".csv";
        try {
            Files.createDirectories(Paths.get(this.logfileName).getParent());
            Files.createDirectories(Paths.get(this.summaryFileName).getParent());
            Files.write(Paths.get(this.logfileName), new byte[0]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FileUtil.appendUtf8String("# new " + logfileName + " log\n", this.logfileName);
    }

    public GetlLogger() {
        try {
            Files.createDirectories(Paths.get(this.logfileName).getParent());
            Files.createDirectories(Paths.get(this.summaryFileName).getParent());
            Files.write(Paths.get(this.logfileName), new byte[0]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        FileUtil.appendUtf8String("# new " + logfileName + " log\n", this.logfileName);
    }

    public void debugInfo(String info) {
        this.debugInfo(info, 0);
    }

    public void info(String info) {
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        simpleDateFormat.setTimeZone(java.util.TimeZone.getTimeZone("GMT+8"));
        String formattedDate = simpleDateFormat.format(date);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(formattedDate).append("::").append(info).append("\n");
        System.out.println(stringBuilder.toString());
        FileUtil.appendUtf8String(stringBuilder.toString() + "\n\n", this.logfileName);
    }

    public void debugInfo(String info, long time) {
        long currentTimeMillis = System.currentTimeMillis();
        Date date = new Date(currentTimeMillis);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        simpleDateFormat.setTimeZone(java.util.TimeZone.getTimeZone("GMT+8"));
        String formattedDate = simpleDateFormat.format(date);

        // Calculate memory usage
        long totalMemory = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        long used = totalMemory - free;

        // Cache time and memory data for this info type
        infoTimeCache.computeIfAbsent(info, k -> new ArrayList<>()).add(time);
        infoMemoryCache.computeIfAbsent(info, k -> new ArrayList<>()).add(used);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\n## ");
        stringBuilder.append(info);
        stringBuilder.append("\n```");
        stringBuilder.append("\ncurrent time: ").append(formattedDate);
        stringBuilder.append("\nused time: ").append(NumberFormat.getInstance(Locale.US).format(time)).append(" ms");
        long max = Runtime.getRuntime().maxMemory();
        stringBuilder.append("\nJVM max Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(max)).append(" B");
        stringBuilder.append("\nJVM current total Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(totalMemory)).append(" B");
        stringBuilder.append("\nJVM current free Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(free)).append(" B");
        stringBuilder.append("\nUsed Memory (Byte): ").append(NumberFormat.getInstance(Locale.US).format(used)).append(" B");
        stringBuilder.append("\n```");
        System.out.println(stringBuilder.toString());
        FileUtil.appendUtf8String(stringBuilder.toString() + "\n\n", this.logfileName);
    }

    /**
     * Calculate and output statistics for all cached info logs.
     * This method should be called when logging is complete (similar to a destructor).
     */
    public void close() {
        if (infoTimeCache.isEmpty()) {
            return;
        }

        // Generate CSV summary file
        StringBuilder csvContent = new StringBuilder();

        // CSV header
        csvContent.append("Info,Execution Count,Average Time (ms),Min Time (ms),Max Time (ms),Total Time (ms),Average Memory (B),Min Memory (B),Max Memory (B),Max Memory (MB)\n");

        // Sort info types alphabetically for consistent output
        List<String> sortedInfoTypes = new ArrayList<>(infoTimeCache.keySet());
        Collections.sort(sortedInfoTypes);

        // Generate markdown statistics
        StringBuilder statistics = new StringBuilder();
        statistics.append("\n---\n========================\n# Statistics Summary\n========================\n");

        for (String info : sortedInfoTypes) {
            List<Long> times = infoTimeCache.get(info);
            List<Long> memories = infoMemoryCache.get(info);

            if (times == null || times.isEmpty()) {
                continue;
            }

            // Calculate statistics for time
            long totalTime = 0;
            long minTime = Long.MAX_VALUE;
            long maxTime = Long.MIN_VALUE;
            for (Long time : times) {
                totalTime += time;
                minTime = Math.min(minTime, time);
                maxTime = Math.max(maxTime, time);
            }
            double avgTime = (double) totalTime / times.size();

            // Calculate statistics for memory
            long totalMemory = 0;
            long minMemory = Long.MAX_VALUE;
            long maxMemory = Long.MIN_VALUE;
            for (Long memory : memories) {
                totalMemory += memory;
                minMemory = Math.min(minMemory, memory);
                maxMemory = Math.max(maxMemory, memory);
            }
            double avgMemory = (double) totalMemory / memories.size();

            // Add CSV row (escape commas in info string)
            String escapedInfo = info.replace("\"", "\"\"");
            if (escapedInfo.contains(",")) {
                escapedInfo = "\"" + escapedInfo + "\"";
            }
            csvContent.append(escapedInfo).append(",")
                    .append(times.size()).append(",")
                    .append((long) avgTime).append(",")
                    .append(minTime).append(",")
                    .append(maxTime).append(",")
                    .append(totalTime).append(",")
                    .append((long) avgMemory).append(",")
                    .append(minMemory).append(",")
                    .append(maxMemory).append(",")
                    .append(String.format("%.2f", maxMemory / (1024.0 * 1024.0))).append("\n");

            // Build statistics output (markdown format)
            statistics.append("## ").append(info).append("\n");
            statistics.append("```\n");
            statistics.append("Execution count: ").append(times.size()).append("\n");
            statistics.append("\nTime Statistics:\n");
            statistics.append("  Average time: ").append(NumberFormat.getInstance(Locale.US).format((long) avgTime)).append(" ms\n");
            statistics.append("  Min time: ").append(NumberFormat.getInstance(Locale.US).format(minTime)).append(" ms\n");
            statistics.append("  Max time: ").append(NumberFormat.getInstance(Locale.US).format(maxTime)).append(" ms\n");
            statistics.append("  Total time: ").append(NumberFormat.getInstance(Locale.US).format(totalTime)).append(" ms\n");
            statistics.append("\nMemory Statistics:\n");
            statistics.append("  Average memory: ").append(NumberFormat.getInstance(Locale.US).format((long) avgMemory)).append(" B\n");
            statistics.append("  Min memory: ").append(NumberFormat.getInstance(Locale.US).format(minMemory)).append(" B\n");
            statistics.append("  Max memory: ").append(NumberFormat.getInstance(Locale.US).format(maxMemory)).append(" B\n");
            statistics.append("  Max memory (MB): ").append(String.format("%.2f", maxMemory / (1024.0 * 1024.0))).append(" MB\n");
            statistics.append("```\n\n");
        }

        // Write CSV file
        try {
            Files.write(Paths.get(summaryFileName), csvContent.toString().getBytes("UTF-8"));
            System.out.println("CSV summary written to: " + summaryFileName);
        } catch (IOException e) {
            System.err.println("Error writing CSV summary: " + e.getMessage());
        }

        System.out.println(statistics.toString());
        FileUtil.appendUtf8String(statistics.toString(), this.logfileName);

        // Clear caches
        infoTimeCache.clear();
        infoMemoryCache.clear();
    }

    /**
     * Get statistics for a specific info type.
     *
     * @param info The info type to get statistics for
     * @return A map containing "avgTime", "avgMemory", "count", or null if no data exists
     */
    public Map<String, Object> getStatistics(String info) {
        List<Long> times = infoTimeCache.get(info);
        List<Long> memories = infoMemoryCache.get(info);

        if (times == null || times.isEmpty()) {
            return null;
        }

        long totalTime = 0;
        for (Long time : times) {
            totalTime += time;
        }
        double avgTime = (double) totalTime / times.size();

        long totalMemory = 0;
        for (Long memory : memories) {
            totalMemory += memory;
        }
        double avgMemory = (double) totalMemory / memories.size();

        Map<String, Object> stats = new HashMap<>();
        stats.put("avgTime", avgTime);
        stats.put("avgMemory", avgMemory);
        stats.put("count", times.size());
        stats.put("totalTime", totalTime);
        stats.put("totalMemory", totalMemory);

        return stats;
    }
}
