package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.converter.RMConverter;
import com.getl.converter.async.AsyncRM2UMG;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.util.DebugUtil;

import java.sql.SQLException;

public class Q1 {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        DebugUtil.DebugInfo("BEGIN TO TEST Q1");
        System.out.println(System.currentTimeMillis());
        RMGraph rmGraph = new RMGraph();
        RMConverter rmConverter = new RMConverter(new UnifiedGraph(), rmGraph);
        MysqlOp.asyncRM2UMG = new AsyncRM2UMG(rmConverter);
        MysqlSessions sessions = new MysqlSessions(CommonConstant.JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        long begin = System.currentTimeMillis();
        long beginall = System.currentTimeMillis();
        MysqlOp.query(sessions, rmGraph);
        long t1 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========QUERY FROM MYSQL END [" + t1 + "]=========");
        System.out.println("Line Size " + rmGraph.getLines().size());
        MysqlOp.asyncRM2UMG.shutdown();
        UnifiedGraph unifiedGraph = MysqlOp.asyncRM2UMG.getUnifiedGraph();
        DebugUtil.DebugInfo("RM2UGM " + (System.currentTimeMillis() - begin));
        System.out.println("Pairs: " + unifiedGraph.getCache().size());
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
