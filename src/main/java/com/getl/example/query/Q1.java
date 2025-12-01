package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;

import java.sql.SQLException;

public class Q1 extends Runnable {
    public static void main(String[] args) {
        new Q1().accept();
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN TO TEST Q1");
            UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRMDataset(logger);
            System.out.println("Pairs: " + unifiedGraph.getCache().size());
            long begin = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            logger.debugInfo("GC" + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            GraphAPI graphAPI = GraphAPI.open(unifiedGraph);
            graphAPI.refreshRDF();
            logger.debugInfo("UGM2RDF end " + (System.currentTimeMillis() - begin));
            System.out.println("RDF SIZE : " + graphAPI.getRDF().size());
        } catch (SQLException | ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("QUERY 1");
    }
}
