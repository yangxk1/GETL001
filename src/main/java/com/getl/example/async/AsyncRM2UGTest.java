package com.getl.example.async;

import com.getl.constant.CommonConstant;
import com.getl.converter.RMConverter;
import com.getl.converter.async.AsyncRM2UMG;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.ug.UnifiedGraph;

import java.sql.SQLException;
import java.util.Map;

public class AsyncRM2UGTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("BEGIN TO TEST RDF 2 MG");
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
        System.out.println("Line Size" + rmGraph.getLines().size());
        MysqlOp.asyncRM2UMG.shutdown();
        long t2 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========RM 2 kv END [" + t2 + "]=========");
        System.out.println("pipeline " + (System.currentTimeMillis() - beginall));
        UnifiedGraph unifiedGraph = MysqlOp.asyncRM2UMG.getUnifiedGraph();
        rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addKVGraphToRMModel();
        long t3 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========Convert to RM END [" + t3 + "]=========");
        System.out.println(rmConverter.rmGraph.getLines().size());
    }
}
