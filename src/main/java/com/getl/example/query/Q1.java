package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.converter.RMConverter;
import com.getl.converter.async.AsyncRM2UMG;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.util.DebugUtil;

import java.sql.SQLException;

public class Q1 extends Runnable {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        DebugUtil.DebugInfo("BEGIN TO TEST Q1");
        UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRMDataset();
        System.out.println("Pairs: " + unifiedGraph.getCache().size());
        long begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open(unifiedGraph);
        graphAPI.refreshRDF();
        DebugUtil.DebugInfo("UGM2RDF end " + (System.currentTimeMillis() - begin));
        System.out.println("RDF SIZE : " + graphAPI.getRDF().size());
    }


    @Override
    public void forward() {
        try {
            main(null);
        } catch (SQLException | ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
