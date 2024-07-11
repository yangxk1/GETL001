package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.converter.RMConverter;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.util.DebugUtil;

import java.sql.SQLException;

public class Q1 {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        DebugUtil.DebugInfo("BEGIN TO TEST Q1");
        RMGraph rmGraph = new RMGraph();
        MysqlSessions sessions = new MysqlSessions(CommonConstant.JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        long begin = System.currentTimeMillis();
        MysqlOp.query(sessions, rmGraph);
        DebugUtil.DebugInfo("QUERY FROM MYSQL END " + (System.currentTimeMillis() - begin));
        System.out.println("Line Size" + rmGraph.getLines().size());
        begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addRMModelToKVGraph();
        unifiedGraph = rmConverter.unifiedGraph;
        DebugUtil.DebugInfo("RM2UGM " + (System.currentTimeMillis() - begin));
        System.out.println("kvPairs: " + unifiedGraph.getCache().size());
        begin = System.currentTimeMillis();
        rmGraph = null;
        rmConverter = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open(unifiedGraph);
        graphAPI.refreshRDF();
        DebugUtil.DebugInfo("UGM2RDF end " + (System.currentTimeMillis() - begin));
        System.out.println("RDF SIZE : " + graphAPI.getRDF().size());
    }
}
