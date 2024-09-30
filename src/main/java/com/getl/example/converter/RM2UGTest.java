package com.getl.example.converter;

import com.getl.converter.RMConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.RM.*;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;

import java.sql.SQLException;

public class RM2UGTest extends Runnable {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRMDataset();
        long begin = System.currentTimeMillis();
        RMConverter rmConverter = new RMConverter(unifiedGraph, new RMGraph().setSchemas(LoadUtil.schema));
        rmConverter.addUGMToRMModel();
        DebugUtil.DebugInfo("ugm 2 rm time: " + (System.currentTimeMillis() - begin) + " ms");
    }

    @Override
    public void forward() {
        try {
            main(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
