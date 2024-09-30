package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;

import java.sql.SQLException;

public class Q1 extends Runnable {
    public static void main(String[] args) {
        new Q1().accept();
    }

    @Override
    public void accept() {
        try {
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
        } catch (SQLException | ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
