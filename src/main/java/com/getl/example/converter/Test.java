package com.getl.example.converter;

import com.getl.constant.CommonConstant;
import com.getl.converter.ConverterUtils;
import com.getl.converter.RMConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;

import java.sql.SQLException;

public class Test extends Runnable {

    public static void main(String[] args) {
        new Test().accept();
    }

    @Override
    protected void run() {
        RMGraph rmGraph = new RMGraph();
        MysqlSessions sessions = null;
        try {
            sessions = new MysqlSessions(CommonConstant.LDBC_JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            MysqlOp.loadRMGraph(sessions, rmGraph);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        long begin = System.currentTimeMillis();
        testUG(rmGraph);
        logger.debugInfo("UG Conversion ", System.currentTimeMillis() - begin);
        System.gc();
        begin = System.currentTimeMillis();
        testMG(rmGraph);
        logger.debugInfo("MG Conversion ", System.currentTimeMillis() - begin);
        System.gc();
        begin = System.currentTimeMillis();
        testUG(rmGraph);
        logger.debugInfo("UG Conversion ", System.currentTimeMillis() - begin);
        System.gc();
        begin = System.currentTimeMillis();
        testMG(rmGraph);
        logger.debugInfo("MG Conversion ", System.currentTimeMillis() - begin);
    }

    private void testUG(RMGraph rmGraph) {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRM(rmGraph);
        logger.debugInfo("RM2UG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromUGGraph(unifiedGraph, rmGraph.getSchemas());
        logger.debugInfo("UG2RM", System.currentTimeMillis() - begin);

    }

    private void testMG(RMGraph rmGraph) {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGFromRM(rmGraph);
        logger.debugInfo("RM2MG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromMG(mGraph, rmGraph.getSchemas());
        logger.debugInfo("MG2RM", System.currentTimeMillis() - begin);
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("RM2UGTest");
    }
}
