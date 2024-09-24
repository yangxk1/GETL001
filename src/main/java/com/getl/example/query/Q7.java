package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.example.Runnable;
import com.getl.example.utils.LDBC2UGUtil;
import com.getl.example.utils.RandomWalk;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.sql.SQLException;
import java.util.List;

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
        Long count = lpgGraph.traversal().V().hasLabel("Person").count().next();
        RandomWalk randomWalk = new RandomWalk(lpgGraph);
        LPGGraph resultGraph = new LPGGraph();
        List<List<Vertex>> lists = randomWalk.asyncForward(Math.toIntExact(count), 3);
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
        System.out.println("rand walk end " + (System.currentTimeMillis() - begin));
        double v = 1.0 * (System.currentTimeMillis() - begin) / count;
        System.out.println(v);
        System.out.println("vertices count: " + resultGraph.getVertices().size());
        System.out.println("vertices count: " + resultGraph.getEdges().size());
    }

    @Override
    public String init() {
        return validateParams(CommonConstant.JDBC_PASSWORD, CommonConstant.JDBC_USERNAME, CommonConstant.LDBC_JDBC_URL, CommonConstant.LDBC_RDF_FILES_URL, CommonConstant.LPG_FILES_BASE_URL);
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