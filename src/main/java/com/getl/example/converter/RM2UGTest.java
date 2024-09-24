package com.getl.example.converter;

import com.getl.constant.CommonConstant;
import com.getl.converter.RMConverter;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.ug.UnifiedGraph;

import java.sql.SQLException;
import java.util.Map;

public class RM2UGTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("BEGIN TO TEST RDF 2 MG");
        System.out.println(System.currentTimeMillis());
        RMGraph rmGraph = new RMGraph();
        MysqlSessions sessions = new MysqlSessions(CommonConstant.JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        long begin = System.currentTimeMillis();
        MysqlOp.query(sessions, rmGraph);
        long t1 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========QUERY FROM MYSQL END [" + t1 + "]=========");
        System.out.println("Line Size" + rmGraph.getLines().size());
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addRMModelToUGM();
        long t2 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========RM 2 ugm END [" + t2 + "]=========");
        //GC
        Map<String, Schema> schemas = rmGraph.getSchemas();
        rmGraph = new RMGraph();
        rmGraph.setSchemas(schemas);
        unifiedGraph.gc();
        rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addUGMToRMModel();
        long t3 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========Convert to RM END [" + t3 + "]=========");
        System.out.println(rmConverter.rmGraph.getLines().size());
        //gc
        unifiedGraph = new UnifiedGraph();
        rmConverter = new RMConverter(unifiedGraph, rmGraph);
//        sessions = new MysqlSessions(CommonConstant.JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
//        MysqlOp.createSchema(sessions);
////        MysqlOp.write(sessions, rmGraph);
//        long t4 = System.currentTimeMillis() - begin;
//        System.out.println("==========Write RM END [" + t4 + "]=========");
    }
}
