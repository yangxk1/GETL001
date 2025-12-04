package com.getl.example.test;

import com.getl.constant.CommonConstant;
import com.getl.converter.ConverterUtils;
import com.getl.example.Runnable;
import com.getl.io.AsyncLPGParser;
import com.getl.model.MG.MGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.CompletableFuture;

public class PropertyGraphTest extends Runnable {
    public static void main(String[] args) {
        new PropertyGraphTest().accept();
    }

    private Graph graphData;

    @Override
    protected void run() {
        loadData();
        System.out.println("================================");
        UG();
        MG();
        SG();
        System.out.println("================================");
        UG();
        MG();
        SG();
        System.out.println("================================");
        UG();
        MG();
        SG();
    }

    private void loadData() {
        AsyncLPGParser asyncParser = new AsyncLPGParser(8);
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
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
            graphData = asyncParser.getGraph();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        } finally {
            // 关键修复：必须关闭 AsyncLPGParser 以终止后台线程
            asyncParser.shutdown();
        }
    }

    private void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGFromTinkerPopGraph(graphData);
        logger.debugInfo("TinkerPopGraph2UG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromUG(unifiedGraph);
        logger.debugInfo("UG2TinkerPopGraph", System.currentTimeMillis() - begin);
    }

    private void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGFromTinkerPopGraph(graphData);
        logger.debugInfo("TinkerPopGraph2MG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromMG(mGraph);
        logger.debugInfo("MG2TinkerPopGraph", System.currentTimeMillis() - begin);
    }

    private void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromTinkerPopGraph(graphData);
        logger.debugInfo("TinkerPopGraph2SG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromSG(ogDataset);
        logger.debugInfo("SG2TinkerPopGraph", System.currentTimeMillis() - begin);
    }

    @Override
    protected String loggerName() {
        return "PropertyGraphTest";
    }
}
