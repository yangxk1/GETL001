package com.getl.example.async;

import com.getl.Graph;
import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.converter.async.AsyncRM2UMG;
import com.getl.io.LPGParser;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class LBDC2UGTest {
    public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException {
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        long begin = System.currentTimeMillis();
        loadFromRM(unifiedGraph);
        Runtime.getRuntime().gc();
        loadFromRDF(unifiedGraph);
        Runtime.getRuntime().gc();
        loadFromPG(unifiedGraph);
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("load time: " + (System.currentTimeMillis() - begin) + " ms");
        System.out.println("ug count: " + unifiedGraph.getCache().size());
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
        graphAPI.refreshLPG();
        LPGGraph lpgGraph = graphAPI.getGraph().getLpgGraph();
        DebugUtil.DebugInfo("convert 2 pg time: " + (System.currentTimeMillis() - begin) + " ms");
        System.out.println("Vertices count: " + lpgGraph.getVertices().size());
        System.out.println("Edges count: " + lpgGraph.getEdges().size());
        System.out.println();
    }

    public static void loadFromRDF(UnifiedGraph unifiedGraph) {
        String RDF_URL = CommonConstant.LBDC_RDF_FILES_URL;
        DebugUtil.DebugInfo("BEGIN TO LOAD RDF");
        Graph graph = new Graph(unifiedGraph);
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
        graph.handleRDFModel();
        DebugUtil.DebugInfo("RDF2UGM END " + (System.currentTimeMillis() - begin));
    }

    public static void loadFromRM(UnifiedGraph unifiedGraph) throws SQLException, ClassNotFoundException {
        DebugUtil.DebugInfo("BEGIN TO LOAD RM");
        RMGraph rmGraph = new RMGraph();
        RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
        MysqlOp.asyncRM2UMG = new AsyncRM2UMG(rmConverter);
        MysqlSessions sessions = new MysqlSessions(CommonConstant.LBDC_JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        long begin = System.currentTimeMillis();
        long beginall = System.currentTimeMillis();
        MysqlOp.query(sessions, rmGraph);
        long t1 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        DebugUtil.DebugInfo("QUERY FROM MYSQL END [" + t1 + "ms]");
        System.out.println("Line Size " + rmGraph.getLines().size());
        MysqlOp.asyncRM2UMG.shutdown();
        long t2 = System.currentTimeMillis() - begin;
        begin = System.currentTimeMillis();
        System.out.println("RM 2 ugm END [" + t2 + "ms]");
        DebugUtil.DebugInfo("rm pipeline " + (System.currentTimeMillis() - beginall));
    }

    public static void loadFromPG(UnifiedGraph unifiedGraph) throws InterruptedException {
        DebugUtil.DebugInfo("BEGIN TO LOAD PG");
        LPGParser lpgParser = new LPGParser(new TinkerPopConverter(unifiedGraph, null));
        long begin = System.currentTimeMillis();
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        //EDGE
        lpgParser.latchSize(20);
        lpgParser.loadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place").commit2Converter();

        lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT).commit2Converter();
        DebugUtil.DebugInfo("load pg end " + (System.currentTimeMillis() - begin));
        lpgParser.waitAll();
        DebugUtil.DebugInfo("commit pg end " + (System.currentTimeMillis() - begin));
        lpgParser.getAsyncPG2UMG().shutdown();
        DebugUtil.DebugInfo("convert to ugm end " + (System.currentTimeMillis() - begin));
    }
}
