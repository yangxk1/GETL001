package com.getl.experiment;

import com.getl.constant.CommonConstant;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.TinkerPopConverter;
import com.getl.io.AsyncLPGParser;
import com.getl.io.LPGParser;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.Model;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class DataLoadUtils {

    public static Graph loadTinkerPopGraphQ7() {
        AsyncLPGParser asyncParser = new AsyncLPGParser(12);
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";

        try {
            // 异步加载所有顶点文件
            CompletableFuture<Void> edgeFutures = CompletableFuture.allOf(
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place"),

                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.MILLI),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.MILLI),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.MILLI),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.MILLI),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT));

            // 等待所有文件读取任务完成
            CompletableFuture.allOf(edgeFutures).join();

            // 等待所有数据写入完成
            asyncParser.awaitCompletion();
            return asyncParser.getGraph();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        } finally {
            // 关键修复：必须关闭 AsyncLPGParser 以终止后台线程
            asyncParser.shutdown();
        }
    }

    public static Graph loadTinkerPopGraphSf10() {
        AsyncLPGParser asyncParser = new AsyncLPGParser(24);
        String BASE_URL = CommonConstant.EXTEND_LDBC;
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";

        try {
            // 异步加载所有顶点文件
            CompletableFuture<Void> vertexFutures = CompletableFuture.allOf(
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation"),
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place"),
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag"),
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass"),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", AsyncLPGParser.LONG),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", AsyncLPGParser.LONG),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", AsyncLPGParser.LONG, "creationDate", AsyncLPGParser.LONG),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", AsyncLPGParser.LONG, "length", AsyncLPGParser.INT),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person"),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person")
            );

            // 异步加载所有边文件
            CompletableFuture<Void> edgeFutures = CompletableFuture.allOf(
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", AsyncLPGParser.LONG),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", AsyncLPGParser.LONG),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", AsyncLPGParser.LONG),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", AsyncLPGParser.LONG),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", AsyncLPGParser.INT),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", AsyncLPGParser.INT),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place")
            );

            // 等待所有文件读取任务完成
            CompletableFuture.allOf(vertexFutures, edgeFutures).join();

            // 等待所有数据写入完成
            asyncParser.awaitCompletion();
            return asyncParser.getGraph();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        } finally {
            // 关键修复：必须关闭 AsyncLPGParser 以终止后台线程
            asyncParser.shutdown();
        }
    }

    public static Graph loadTinkerPopGraph() {
        return loadTinkerPopGraph(CommonConstant.LPG_FILES_BASE_URL);
    }

    public static Graph loadTinkerPopGraph(String baseUrl) {
        AsyncLPGParser asyncParser = new AsyncLPGParser(8);
        String BASE_URL = baseUrl;
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";

        try {
            // 异步加载所有顶点文件
            CompletableFuture<Void> vertexFutures = CompletableFuture.allOf(
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation"),
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place"),
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag"),
                    asyncParser.asyncLoadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass"),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", AsyncLPGParser.DATE),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", AsyncLPGParser.DATE),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", AsyncLPGParser.DATE, "creationDate", AsyncLPGParser.DATE),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", AsyncLPGParser.DATE, "length", AsyncLPGParser.INT),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person"),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person")
            );

            // 异步加载所有边文件
            CompletableFuture<Void> edgeFutures = CompletableFuture.allOf(
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass"),
                    asyncParser.asyncLoadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", AsyncLPGParser.DATE),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", AsyncLPGParser.DATE),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", AsyncLPGParser.DATE),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", AsyncLPGParser.DATE),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", AsyncLPGParser.INT),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", AsyncLPGParser.INT),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place")
            );

            // 等待所有文件读取任务完成
            CompletableFuture.allOf(vertexFutures, edgeFutures).join();

            // 等待所有数据写入完成
            asyncParser.awaitCompletion();
            return asyncParser.getGraph();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        } finally {
            // 关键修复：必须关闭 AsyncLPGParser 以终止后台线程
            asyncParser.shutdown();
        }
    }

    public static Model loadRDFModel() {
        String RDF_URL = CommonConstant.RDF_FILES_BASE_URL;
        return loadRDFModel(RDF_URL);
    }

    public static Model loadRDFModel(String baseUrl) {
        com.getl.Graph graph = new com.getl.Graph();
        String RDF_URL = baseUrl;
        File resource = new File(RDF_URL);
        try {
            System.out.println("File: " + RDF_URL);
            graph.readRDFFile(RdfDataFormat.TURTLE, resource);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return graph.getRdfModel();
    }

    public static RMGraph loadRMGraph() {
        return loadRMGraph(CommonConstant.JDBC_URL);
    }

    public static RMGraph loadRMGraph(String baseUrl) {
        RMGraph rmGraph = new RMGraph();
        MysqlSessions sessions = null;
        try {
            sessions = new MysqlSessions(baseUrl, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            MysqlOp.loadRMGraph(sessions, rmGraph);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return rmGraph;
    }
}
