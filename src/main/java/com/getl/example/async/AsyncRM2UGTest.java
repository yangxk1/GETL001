package com.getl.example.async;

import com.getl.constant.CommonConstant;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.RMConverter;
import com.getl.converter.async.AsyncRM2UMG;
import com.getl.example.Runnable;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.RM.*;
import com.getl.model.ug.UnifiedGraph;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

public class AsyncRM2UGTest extends Runnable {
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
        System.out.println("Line Size " + rmGraph.getLines().size());
        MysqlOp.asyncRM2UMG.shutdown();
        long t2 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========RM 2 ugm END [" + t2 + "]=========");
        System.out.println("pipeline " + (System.currentTimeMillis() - beginall));
        HashSet<String> lines = rmGraph.getLines().values().stream().map(Line::getId).collect(Collectors.toCollection(HashSet::new));
        rmGraph = null;
        Runtime.getRuntime().gc();
        UnifiedGraph unifiedGraph = MysqlOp.asyncRM2UMG.getUnifiedGraph();
//        LPGGraph lpgGraphByUGM = new LPGGraphConverter(unifiedGraph, null, new HashMap<>()).createLPGGraphByUGM();
//        System.out.println(lpgGraphByUGM.getVertices().size());
        rmConverter = new RMConverter(unifiedGraph, new RMGraph().setSchemas(rmGraph.getSchemas()));
        rmConverter.addUGMToRMModel();
        long t3 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("==========Convert to RM END [" + t3 + "]=========");
        HashSet<String> lines1 = rmConverter.rmGraph.getLines().values().stream().filter(line -> !line.getTableName().equals("user")).map(Line::getId).collect(Collectors.toCollection(HashSet::new));
        lines1.removeAll(lines);
        System.out.println(lines1);
    }

    @Override
    public String init() {
        if (StringUtils.isBlank(CommonConstant.JDBC_URL)) {
            return CommonConstant.JDBC_URL;
        }
        if (StringUtils.isBlank(CommonConstant.JDBC_USERNAME)) {
            return CommonConstant.JDBC_USERNAME;
        }
        if (StringUtils.isBlank(CommonConstant.JDBC_PASSWORD)) {
            return CommonConstant.JDBC_PASSWORD;
        }
        return "";
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
