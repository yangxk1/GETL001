package com.getl.example.converter;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.TinkerPopConverter;
import com.getl.io.LPGParser;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;

public class PG2UGTest {

    public static void main(String[] args) {
        System.out.println("BEGIN TO TEST PG2UGM");
        System.out.println(System.currentTimeMillis());
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
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.DATE);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.DATE);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.DATE, "creationDate", LPGParser.DATE);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.DATE, "length", LPGParser.INT);
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
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.DATE);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.DATE);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.DATE);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.DATE);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place");

        System.out.println("load pg end " + (System.currentTimeMillis() - begin));
        System.out.println(System.currentTimeMillis());
//        begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = (new TinkerPopConverter(null, lpgParser.getGraph())).createUGMFromTinkerPopGraph();
        DebugUtil.DebugInfo("2 UGM end " + (System.currentTimeMillis() - begin));
        System.out.println(unifiedGraph.getCache().size());
        //GC
        lpgParser.setGraph(null);
        lpgParser = null;
        System.out.println("PG2UGM end " + (System.currentTimeMillis() - begin));
        System.out.println(System.currentTimeMillis());
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.addUGMPairs(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
        graphAPI.refreshLPG();
        System.out.println("UGM2PG END" + (System.currentTimeMillis() - begin));
        System.out.println(System.currentTimeMillis());
        System.out.println("Vertex size : " + graphAPI.getGraph().getLpgGraph().getVertices().size());
        System.out.println(System.currentTimeMillis());
        System.out.println("Edge size : " + graphAPI.getGraph().getLpgGraph().getEdges().size());
        System.out.println(System.currentTimeMillis());
    }
}
