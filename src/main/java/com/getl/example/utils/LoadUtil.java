package com.getl.example.utils;

import com.getl.Graph;
import com.getl.constant.CommonConstant;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.converter.async.AsyncRM2UMG;
import com.getl.io.LPGParser;
import com.getl.model.RM.*;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class LoadUtil {
    public static Map<String, Schema> schema;

    public static UnifiedGraph loadUGFromPGFiles(GetlLogger logger) throws InterruptedException {
        System.out.println("BEGIN TO LOAD PG FILES, CURRENT TIME: " + System.currentTimeMillis() + "ms");
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        LPGParser lpgParser = new LPGParser(new TinkerPopConverter(new UnifiedGraph(), null));
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        lpgParser.latchSize(8);
        lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass").commit2Converter();
        //DYNAMIC
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.MILLI, "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person");
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person").commit2Converter();

        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.MILLI).commit2Converter();
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.MILLI).commit2Converter();
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.MILLI, "length", LPGParser.INT).commit2Converter();
        lpgParser.waitAll();
        //EDGE
        lpgParser.latchSize(23);
        lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass").commit2Converter();
        //DYNAMIC
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.MILLI).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.MILLI).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.MILLI).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.MILLI).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place").commit2Converter();
        logger.debugInfo("load pg files end " + (System.currentTimeMillis() - begin) + " ms");
        lpgParser.waitAll();
        logger.debugInfo("commit pg end " + (System.currentTimeMillis() - begin) + " ms");
        lpgParser.getAsyncPG2UMG().shutdown();
        logger.debugInfo("pg to ugm end " + (System.currentTimeMillis() - begin) + " ms");
        return lpgParser.getAsyncPG2UMG().getUnifiedGraph();
    }

    public static UnifiedGraph loadUGFromRMDataset(GetlLogger logger) throws SQLException, ClassNotFoundException, InterruptedException {
        System.out.println("BEGIN TO LOAD RM DATASET, CURRENT TIME: " + System.currentTimeMillis() + "ms");
        RMConverter rmConverter = new RMConverter(new UnifiedGraph(), new RMGraph());
        MysqlOp.asyncRM2UMG = new AsyncRM2UMG(rmConverter);
        MysqlSessions sessions = new MysqlSessions(CommonConstant.JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        long begin = System.currentTimeMillis();
        MysqlOp.query(sessions, rmConverter.rmGraph);
        logger.debugInfo("load RM end " + (System.currentTimeMillis() - begin) + " ms");
        MysqlOp.waitAll();
        logger.debugInfo("commit RM end " + (System.currentTimeMillis() - begin) + " ms");
        MysqlOp.asyncRM2UMG.shutdown();
        logger.debugInfo("RM 2 ugm END " + (System.currentTimeMillis() - begin) + " ms");
        schema = rmConverter.rmGraph.getSchemas();
        return MysqlOp.asyncRM2UMG.getUnifiedGraph();
    }

    public static UnifiedGraph loadUGFromRDFFile(GetlLogger logger) {
        System.out.println("BEGIN TO LOAD RDF FILE, CURRENT TIME: " + System.currentTimeMillis() + "ms");
        String RDF_URL = CommonConstant.RDF_FILES_BASE_URL;
        Graph graph = new Graph();
        long begin = System.currentTimeMillis();
        File resource = new File(RDF_URL);
        try {
            System.out.println("read " + RDF_URL);
            graph.readRDFFile(RdfDataFormat.TURTLE, resource);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debugInfo("READ RDF END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graph.labelPredicate("http://dbpedia.org/ontology/type");
        graph.handleRDFModel();
        logger.debugInfo("RDF 2 ugm END " + (System.currentTimeMillis() - begin));
        return graph.getUnifiedGraph();
    }


}
