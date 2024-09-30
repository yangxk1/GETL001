package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.io.LPGParser;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGGraph;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;

import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;

public class Q4 extends Runnable {
    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException, InterruptedException {
        DebugUtil.DebugInfo("BEGIN TO TEST Q4");
        UnifiedGraph unifiedGraph = LoadUtil.loadUGFromPGFiles();
        long begin = System.currentTimeMillis();
        //GC
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC " + (System.currentTimeMillis() - begin));
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
        lpgGraph.traversal().V().hasLabel("Person").as("person1")
                .in("comment_hasCreator_person").as("comment")
                .out("comment_replyOf_post").as("post")
                .out("post_hasCreator_person").as("person2")
                .where("person1", P.neq("person2"))
                .not(in("person_isFanOf_person").where(eq("person1")))
                .addE("person_isFanOf_person")
                .from("person1")
                .to("person2")
              //  .select("person1", "comment", "post", "person2")
                .toList();
        DebugUtil.DebugInfo("query end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        unifiedGraph = (new LPGGraphConverter(null, lpgGraph, new HashMap<>())).createUGMFromLPGGraph();
        DebugUtil.DebugInfo("lpg result 2 UGM end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        lpgGraph = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        System.out.println("Pairs: " + unifiedGraph.getCache().size());
        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graphAPI = GraphAPI.open(unifiedGraph);
        graphAPI.refreshRDF();
        DebugUtil.DebugInfo("UGM2RDF end " + (System.currentTimeMillis() - begin));
        System.out.println("RDF SIZE : " + graphAPI.getRDF().size());
    }

    @Override
    public void forward() {
        try {
            main(null);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
