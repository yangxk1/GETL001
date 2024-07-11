package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.TinkerPopConverter;
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

public class Q4 {
    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {
        DebugUtil.DebugInfo("BEGIN TO TEST Q4");
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        LPGParser lpgParser = new LPGParser();
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation");
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place");
        lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag");
        lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass");
        //DYNAMIC
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.MILLI, "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.MILLI, "length", LPGParser.INT);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person");
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person");

        //EDGE
        lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place");
        lpgParser.loadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place");
        lpgParser.loadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass");
        lpgParser.loadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass");
        //DYNAMIC
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place");
        DebugUtil.DebugInfo("load pg end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = (new TinkerPopConverter(null, lpgParser.getGraph())).createKVGraphFromTinkerPopGraph();
        DebugUtil.DebugInfo("PG2UGM end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        //GC
        lpgParser.setGraph(null);
        lpgParser = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setKvGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList("http://kvgraph.example.org/edge");
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
        unifiedGraph = (new LPGGraphConverter(null, lpgGraph, new HashMap<>())).createKVGraphFromLPGGraph();
        DebugUtil.DebugInfo("lpg result 2 UGM end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        lpgGraph = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        System.out.println("kvPairs: " + unifiedGraph.getCache().size());
        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graphAPI = GraphAPI.open(unifiedGraph);
        graphAPI.refreshRDF();
        DebugUtil.DebugInfo("UGM2RDF end " + (System.currentTimeMillis() - begin));
        System.out.println("RDF SIZE : " + graphAPI.getRDF().size());
    }
}
