package com.getl.experiment;

import com.getl.example.Runnable;

public abstract class ExperimentRunner extends Runnable {
    @Override
    protected void run() {
        loadData();
        int times = 3;
        while (times-- > 0) {
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
