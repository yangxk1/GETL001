package com.getl.example.otherModelConverter;

import com.getl.Graph;
import com.getl.api.GraphAPI;
import com.getl.constant.IRINamespace;
import com.getl.constant.RdfDataFormat;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGGraph;
import com.getl.util.DebugUtil;

import java.io.File;
import java.io.IOException;

import static com.getl.constant.CommonConstant.RDF_FILES_BASE_URL;

public class UGRDF2PG {
    public static void main(String[] args) {
        String RDF_URL = RDF_FILES_BASE_URL;
        DebugUtil.DebugInfo("BEGIN TO TEST RDF2PG by UG");
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
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
        graphAPI.refreshLPG();
        LPGGraph lpgGraph = graphAPI.getGraph().getLpgGraph();
        DebugUtil.DebugInfo("UGM2LPG end " + (System.currentTimeMillis() - begin));
        System.out.println("result vertex count: " + lpgGraph.getVertices().size());
        System.out.println("result edge count: " + lpgGraph.getEdges().size());
    }
}
