package com.getl.example.query;

import com.getl.Graph;
import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.LPGGraphConverter;
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

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Q3 {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        String RDF_URL = CommonConstant.RDF_FILES_BASE_URL;
        DebugUtil.DebugInfo("BEGIN TO TEST Q3");
        Graph graph = new Graph();
        long begin = System.currentTimeMillis();
        File resource = new File(RDF_URL);
        try {
            System.out.println("read " + RDF_URL);
            graph.readRDFFile(RdfDataFormat.TURTLE, resource);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DebugUtil.DebugInfo("READ RDF END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graph.labelPredicate("http://dbpedia.org/ontology/type");
        graph.handleRDFModel();
        UnifiedGraph unifiedGraph = graph.getUnifiedGraph();
        DebugUtil.DebugInfo("RDF2UGM END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graph = null;
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
        System.out.println("result vertex count: " + lpgGraph.getVertices().size());
        System.out.println("result edge count: " + lpgGraph.getEdges().size());
        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();

        GraphTraversalSource g = lpgGraph.traversal();// Initialize your GraphTraversalSource
// Q3: Select vertices with a degree greater than 20 and the edges between them
        List<Map<String, Object>> results = g.V()
                .property("_degree", __.bothE().count())
                .has("_degree", P.gt(20)).as("n1")
                .outE().as("e1")
                .inV().has("_degree", P.gt(20)).as("n2")
                .select("n1", "e1", "n2")
                .dedup("e1")
                .toList();
        DebugUtil.DebugInfo("query end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
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
        System.out.println("result vertex count: " + resultGraph.getVertices().size());
        System.out.println("result edge count: " + resultGraph.getEdges().size());
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
    }
}
