package com.getl.example;

import com.getl.util.GetlLogger;

public abstract class Runnable {

    protected GetlLogger logger;

    public void accept() {
        logger = new GetlLogger(loggerName());
        System.out.println("BEGIN TO RUN QUERY");
        System.out.println("WRITE LOG TO " + logger.getLogfileName());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            System.err.println("Sleep interrupted: " + e.getMessage());
        }
        run();
        logger.close();
    }

    protected abstract void run();

    protected abstract String loggerName();
}
