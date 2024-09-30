package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.RMConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LDBC2UGUtil;
import com.getl.example.utils.RandomWalk;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Q7 extends Runnable {
    public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException {
        System.out.println("BEGIN TO TEST Q7 time: " + System.currentTimeMillis());
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        long begin = System.currentTimeMillis();
        LDBC2UGUtil.loadFromRM(unifiedGraph);
        Runtime.getRuntime().gc();
        LDBC2UGUtil.loadFromRDF(unifiedGraph);
        Runtime.getRuntime().gc();
        LDBC2UGUtil.loadFromPG(unifiedGraph);
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("load time: " + (System.currentTimeMillis() - begin) + " ms");
        System.out.println("ug count: " + unifiedGraph.getCache().size());
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
        graphAPI.refreshLPG();
        DebugUtil.DebugInfo("ug 2 lpg end: " + (System.currentTimeMillis() - begin) + " ms");
        LPGGraph lpgGraph = graphAPI.getGraph().getLpgGraph();
        graphAPI = null;
        unifiedGraph = null;
        Runtime.getRuntime().gc();
        begin = System.currentTimeMillis();
        RandomWalk randomWalk = new RandomWalk(lpgGraph);
        LPGGraph resultGraph = new LPGGraph();
        List<List<Vertex>> lists = randomWalk.forward(3);
        System.out.println("rand walk end " + (System.currentTimeMillis() - begin));
        for (int i = 0; i < lists.size(); i++) {
            List<Vertex> vertexIds = lists.get(i);
            if (vertexIds.size() < 3) {
                continue;
            }
            Vertex v1 = vertexIds.get(0);
            Vertex e = vertexIds.get(1);
            Vertex v2 = vertexIds.get(2);
            LPGVertex n1V = resultGraph.getOrCreateVertex(v1.id(), v1.label(), v1.properties());
            LPGVertex n2V = resultGraph.getOrCreateVertex(v2.id(), v2.label(), v2.properties());
            LPGEdge lpgEdge = new LPGEdge(resultGraph, n1V, n2V, "recommend");
            lpgEdge.addPropertyValue("post", e.id());
        }
        System.out.println("collect result to pg end " + (System.currentTimeMillis() - begin));
        System.out.println("vertices count: " + resultGraph.getVertices().size());
        System.out.println("edges count: " + resultGraph.getEdges().size());
        lpgGraph = null;
        Runtime.getRuntime().gc();
        begin = System.currentTimeMillis();
        unifiedGraph = (new LPGGraphConverter(null, resultGraph, new HashMap<>())).createUGMFromLPGGraph();
        DebugUtil.DebugInfo("lpg result 2 ugm end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        resultGraph = null;
        lpgGraph = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        RMGraph rmGraph = new RMGraph();
        rmGraph.addSchema(new Schema("recommend", "Person1", "Person", "Person2", "Person").addColumn("post", Schema.SMALL_TEXT));
        rmGraph.addSchema(new Schema("Person").addColumn("birthday", Schema.DATE)
                .addColumn("firstName", Schema.SMALL_TEXT).addColumn("lastName", Schema.SMALL_TEXT).
                addColumn("gender", Schema.SMALL_TEXT).addColumn("browserUsed", Schema.SMALL_TEXT).
                addColumn("locationIP", Schema.SMALL_TEXT).addColumn("language", Schema.SMALL_TEXT).
                addColumn("creationDate", Schema.DATE).addColumn("email", Schema.MID_LARGE_TEXT));
        RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addUGMToRMModel();
        DebugUtil.DebugInfo("ugm 2 RM end " + (System.currentTimeMillis() - begin));
        System.out.println("Lines: " + rmGraph.getLines().size());
        begin = System.currentTimeMillis();
        MysqlSessions sessions = new MysqlSessions(CommonConstant.LDBC_JDBC_RESULT, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        MysqlOp.createSchema(sessions);
        MysqlOp.write(sessions, rmGraph);
        DebugUtil.DebugInfo("Write RM end " + (System.currentTimeMillis() - begin));
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