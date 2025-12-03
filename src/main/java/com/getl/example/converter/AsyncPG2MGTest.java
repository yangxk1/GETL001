package com.getl.example.converter;

import com.getl.constant.CommonConstant;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.io.AsyncLPGParser;
import com.getl.io.LPGParser;
import com.getl.model.MG.MGraph;

import java.util.concurrent.CompletableFuture;

/**
 * 异步PG到MG转换测试 - 使用并发文件读取优化性能
 */
public class AsyncPG2MGTest {

    public static void main(String[] args) {
        // 测试原始同步版本
        testSyncVersion();

        // 测试新的异步版本
        testAsyncVersion();
    }

    /**
     * 原始同步版本测试（用于对比）
     */
    public static void testSyncVersion() {
        System.out.println("\n========== 同步版本测试 ==========");
        System.out.println("BEGIN TO TEST SYNC PG 2 MG, time: " + System.currentTimeMillis());

        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        LPGParser lpgParser = new LPGParser();
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";

        // 加载顶点
        lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation");
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place");
        lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag");
        lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass");
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.MILLI, "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.MILLI, "length", LPGParser.INT);

        // 加载边
        lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place");
        lpgParser.loadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.MILLI);

        long loadTime = System.currentTimeMillis() - begin;
        System.out.println("同步加载完成，耗时: " + loadTime + " ms");

        begin = System.currentTimeMillis();
        PGMapperI pgMapper = new PGMapperR4j(new MGraph());
        pgMapper.addPGToMG(lpgParser.getGraph());
        lpgParser.setGraph(null);
        lpgParser = null;

        long convertTime = System.currentTimeMillis() - begin;
        System.out.println("PG2MG 转换完成，耗时: " + convertTime + " ms");

        begin = System.currentTimeMillis();
        org.apache.tinkerpop.gremlin.structure.Graph graph = pgMapper.createGraphFromMG();

        long reconvertTime = System.currentTimeMillis() - begin;
        System.out.println("MG2PG 转换完成，耗时: " + reconvertTime + " ms");
        System.out.println("同步版本总耗时: " + (loadTime + convertTime + reconvertTime) + " ms\n");
    }

    /**
     * 异步版本测试
     */
    public static void testAsyncVersion() {
        System.out.println("\n========== 异步版本测试 ==========");
        System.out.println("BEGIN TO TEST ASYNC PG 2 MG, time: " + System.currentTimeMillis());

        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;

        // 创建异步解析器，使用8个读取线程
        AsyncLPGParser asyncParser = new AsyncLPGParser(8);

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
                asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", AsyncLPGParser.MILLI),
                asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", AsyncLPGParser.MILLI),
                asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", AsyncLPGParser.MILLI, "creationDate", AsyncLPGParser.MILLI),
                asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", AsyncLPGParser.MILLI, "length", AsyncLPGParser.INT),
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
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", AsyncLPGParser.MILLI),
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person"),
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag"),
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag"),
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place"),
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", AsyncLPGParser.MILLI),
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", AsyncLPGParser.MILLI),
                asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", AsyncLPGParser.MILLI),
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

            long loadTime = System.currentTimeMillis() - begin;
            System.out.println("异步加载完成，耗时: " + loadTime + " ms");
            System.out.println("统计信息: " + asyncParser.getStatistics());

            // PG2MG转换
            begin = System.currentTimeMillis();
            PGMapperI pgMapper = new PGMapperR4j(new MGraph());
            pgMapper.addPGToMG(asyncParser.getGraph());

            long convertTime = System.currentTimeMillis() - begin;
            System.out.println("PG2MG 转换完成，耗时: " + convertTime + " ms");

            // MG2PG转换
            begin = System.currentTimeMillis();
            org.apache.tinkerpop.gremlin.structure.Graph graph = pgMapper.createGraphFromMG();

            long reconvertTime = System.currentTimeMillis() - begin;
            System.out.println("MG2PG 转换完成，耗时: " + reconvertTime + " ms");
            System.out.println("异步版本总耗时: " + (loadTime + convertTime + reconvertTime) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            asyncParser.shutdown();
        }
    }
}

