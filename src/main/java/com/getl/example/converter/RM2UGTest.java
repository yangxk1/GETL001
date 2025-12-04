package com.getl.example.converter;

import com.getl.converter.RMConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.RM.*;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;

public class RM2UGTest extends Runnable {
    public static void main(String[] args) {
        new RM2UGTest().accept();
    }

    @Override
    protected void run() {
        try {
            UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRMDataset(logger);
            long begin = System.currentTimeMillis();
            RMConverter rmConverter = new RMConverter(unifiedGraph, new RMGraph().setSchemas(LoadUtil.schema));
            rmConverter.addUGMToRMModel();
            logger.debugInfo("ugm 2 rm ", (System.currentTimeMillis() - begin));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String loggerName() {
        return "RM2UGTest";
    }
}
