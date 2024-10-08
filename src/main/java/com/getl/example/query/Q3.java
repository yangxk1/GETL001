package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.IRINamespace;
import com.getl.converter.LPGGraphConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Q3 extends Runnable {

    public static void main(String[] args){
        new Q3().accept();
    }

    @Override
    public void accept() {
        try {
            DebugUtil.DebugInfo("BEGIN TO TEST Q3");
            UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRDFFile();
            long begin = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            GraphAPI graphAPI = GraphAPI.open();
            graphAPI.setUGMGraph(unifiedGraph);
            graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
            graphAPI.refreshLPG();
            LPGGraph lpgGraph = graphAPI.getGraph().getLpgGraph();
            graphAPI.setGraph(null);
            unifiedGraph = null;
            graphAPI = null;
            DebugUtil.DebugInfo("UGM2LPG end " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();

            GraphTraversalSource g = lpgGraph.traversal();// Initialize your GraphTraversalSource
// Q3: Select vertices with a degree greater than 5 and the edges between them
            g.V()
                    .property("_degree", __.bothE().count()).toList();
            List<Map<String, Object>> results = lpgGraph.traversal().V()
                    .has("_degree", P.gt(20)).as("n1")
                    .outE().as("e1")
                    .inV().has("_degree", P.gt(20)).as("n2")
                    .select("n1", "e1", "n2")
                    .dedup("e1")
                    .toList();
            DebugUtil.DebugInfo("query end " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            //  System.out.println(results.size());
            LPGGraph resultGraph = new LPGGraph();
            for (Map<String, Object> result : results) {
                Vertex n1 = (LPGVertex) result.get("n1");
                Edge e1 = (LPGEdge) result.get("e1");
                Vertex n2 = (LPGVertex) result.get("n2");
                LPGVertex n1V = resultGraph.getOrCreateVertex(n1.id(), n1.label(), n1.properties());
                LPGVertex n2V = resultGraph.getOrCreateVertex(n2.id(), n2.label(), n2.properties());
                LPGEdge lpgEdge = new LPGEdge(resultGraph, n1V, n2V, e1.label());
                lpgEdge.setId(e1.id());
            }
            DebugUtil.DebugInfo("collect to lpg end " + (System.currentTimeMillis() - begin));
            lpgGraph = null;
            Runtime.getRuntime().gc();
            DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            unifiedGraph = (new LPGGraphConverter(null, resultGraph, new HashMap<>())).createUGMFromLPGGraph();
            DebugUtil.DebugInfo("lpg result 2 UGM end " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            graphAPI = GraphAPI.open();
            graphAPI.setUGMGraph(unifiedGraph);
            graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
            graphAPI.refreshLPG();
            lpgGraph = graphAPI.getGraph().getLpgGraph();
            DebugUtil.DebugInfo("result UGM 2 LPG end " + (System.currentTimeMillis() - begin));
            System.out.println("lpg vertex count: " + lpgGraph.getVertices().size());
            System.out.println("lpg edge count: " + lpgGraph.getEdges().size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
