package com.getl.example.otherModelConverter;

import com.getl.Graph;
import com.getl.api.GraphAPI;
import com.getl.constant.IRINamespace;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.TinkerPopConverter;
import com.getl.example.Runnable;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGGraph;
import com.getl.util.DebugUtil;

import java.io.File;
import java.io.IOException;

import static com.getl.constant.CommonConstant.RDF_FILES_BASE_URL;

public class UGRDF2PG extends Runnable {
    public static void main(String[] args) {
        new UGRDF2PG().accept();
    }

    @Override
    public void accept() {
        String RDF_URL = RDF_FILES_BASE_URL;
        DebugUtil.DebugInfo("BEGIN TO TEST RDF2PG by UG");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        begin = System.currentTimeMillis();
        graph.labelPredicate("http://dbpedia.org/ontology/type");
        graph.handleRDFModel();
        UnifiedGraph unifiedGraph = graph.getUnifiedGraph();
        DebugUtil.DebugInfo("RDF2UGM END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        System.out.println("ugm vertex count: " + unifiedGraph.getPairs().size());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        graph = null;
        System.gc();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.refreshRDF();
        DebugUtil.DebugInfo("RDF2LPG end " + (System.currentTimeMillis() - begin));
        System.out.println(graphAPI.getRDF().size());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        GraphAPI n_graphAPI = GraphAPI.open();
        n_graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.setGraph(new Graph());
        graphAPI = null;
        System.gc();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        n_graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE_ID);
        n_graphAPI.refreshLPG();
        LPGGraph lpgGraph = n_graphAPI.getGraph().getLpgGraph();
        DebugUtil.DebugInfo("UGM2LPG end " + (System.currentTimeMillis() - begin));
        System.out.println("result vertex count: " + lpgGraph.getVertices().size());
        System.out.println("result edge count: " + lpgGraph.getEdges().size());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        n_graphAPI = null;
        unifiedGraph = null;
        System.gc();
        begin = System.currentTimeMillis();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        unifiedGraph = (new TinkerPopConverter(null, lpgGraph)).createUGMFromTinkerPopGraph();
        DebugUtil.DebugInfo("LPG2UGM end " + (System.currentTimeMillis() - begin));
        System.out.println("ugm vertex count: " + unifiedGraph.getPairs().size());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        lpgGraph = null;
        System.gc();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        }
    }
}
