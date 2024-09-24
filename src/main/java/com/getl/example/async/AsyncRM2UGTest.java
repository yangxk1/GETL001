package com.getl.example.async;

import com.getl.constant.CommonConstant;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.RMConverter;
import com.getl.converter.async.AsyncRM2UMG;
import com.getl.example.Runnable;
import com.getl.example.query.utils.LoadUtil;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.RM.*;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class AsyncRM2UGTest extends Runnable {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRMDataset();
        long begin = System.currentTimeMillis();
        RMConverter rmConverter = new RMConverter(unifiedGraph, new RMGraph().setSchemas(LoadUtil.schema));
        rmConverter.addUGMToRMModel();
        DebugUtil.DebugInfo("ugm 2 rm time: " + (System.currentTimeMillis() - begin) + " ms");
    }

    @Override
    public String init() {
        return validateParams(CommonConstant.JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
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
