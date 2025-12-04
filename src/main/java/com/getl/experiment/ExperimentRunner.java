package com.getl.experiment;

import com.getl.example.Runnable;

public abstract class ExperimentRunner extends Runnable {
    @Override
    protected void run() {
        long beginLoadData = System.currentTimeMillis();
        loadData();
        logger.debugInfo("Load Data", System.currentTimeMillis() - beginLoadData);
        int times = 3;
        while (times-- > 0) {
            logger.info("----- Run " + (4 - times) + " -----");
            UG();
            SG();
            MG();
        }
    }

    protected abstract void loadData();

    protected abstract void UG();

    protected abstract void SG();

    protected abstract void MG();
}
