package com.getl.experiment;

import com.getl.example.Runnable;
import com.getl.util.GetlLogger;

import java.io.PrintWriter;

public abstract class ExperimentRunner extends Runnable {

    private String name;

    public ExperimentRunner(String name) {
        this.name = name;
    }

    @Override
    public void accept() {
        if (logger == null) {
            logger = new GetlLogger(loggerName() + "_" + name);
        }
        System.out.println("WRITE LOG TO " + logger.getLogfileName());
        try {
            run(name);
        } catch (Exception e) {
            logger.info("ERROR RUNNING EXPERIMENT: " + e.getMessage());
            throw new RuntimeException(e);
        }
        logger.close();
    }

    public void run(String name) {
        long beginLoadData = System.currentTimeMillis();
        switch (name) {
            case "UG":
                loadData();
                logger.debugInfo("Load Data", System.currentTimeMillis() - beginLoadData);
                UG();
                break;
            case "SG":
                loadData();
                logger.debugInfo("Load Data", System.currentTimeMillis() - beginLoadData);
                SG();
                break;
            case "MG":
                loadData();
                logger.debugInfo("Load Data", System.currentTimeMillis() - beginLoadData);
                MG();
                break;
            default:
                throw new IllegalArgumentException("Unknown test name: " + name);
        }
    }

    @Override
    protected void run() {
        long beginLoadData = System.currentTimeMillis();
        loadData();
        logger.debugInfo("Load Data", System.currentTimeMillis() - beginLoadData);
        int times = 3;
        while (times-- > 0) {
            logger.info("----- Run " + (4 - times) + " -----");

            // UG测试
            runWithIsolation("UG", () -> UG());

            // SG测试
            runWithIsolation("SG", () -> SG());

            // MG测试
            runWithIsolation("MG", () -> MG());
        }
    }

    /**
     * 增强隔离性地运行测试
     */
    private void runWithIsolation(String testName, TestRunnable test) {
        logger.info("=== Starting " + testName + " test ===");

        // 1. 执行前强制GC，清理之前测试的残留对象
        forceGC();

        // 3. 执行测试
        try {
            test.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 4. 执行后强制GC，清理当前测试产生的对象
        forceGC();

        // 6. 短暂休眠，让系统稳定
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("=== Finished " + testName + " test ===\n");
    }

    /**
     * 强制执行垃圾回收（多次尝试以确保效果）
     */
    protected void forceGC() {
        long before = System.currentTimeMillis();
        System.gc();
        System.runFinalization();
        System.gc();
        long duration = System.currentTimeMillis() - before;
        logger.debugInfo("Force GC duration (ms)", duration);
    }

    @FunctionalInterface
    private interface TestRunnable {
        void run() throws Exception;
    }

    protected abstract void loadData();

    protected abstract void UG();

    protected abstract void SG();

    protected abstract void MG();
}
