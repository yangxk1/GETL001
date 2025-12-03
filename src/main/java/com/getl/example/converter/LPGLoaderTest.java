package com.getl.example.converter;

import com.getl.constant.CommonConstant;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.example.Runnable;
import com.getl.io.AsyncLPGParser;
import com.getl.io.LPGParser;
import com.getl.model.MG.MGraph;
import com.getl.util.GetlLogger;

import java.util.concurrent.CompletableFuture;

public class LPGLoaderTest extends Runnable {
    public static void main(String[] args) {
        new LPGLoaderTest().accept();
    }

    @Override
    protected void run() {
        testAsyncVersion();
        testSyncVersion();
        testAsyncVersion();
        testSyncVersion();
        testAsyncVersion();
        testSyncVersion();
    }

    private static void testSyncVersion() {
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        LPGParser lpgParser = new LPGParser();
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation");
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place");
        lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag");
        lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass");
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place");
        //DYNAMIC
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.STRING);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.STRING);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.STRING, "creationDate", LPGParser.STRING);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.STRING, "length", LPGParser.INT);
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
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.STRING);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.STRING);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.STRING);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.STRING);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place");

        System.out.println("load pg end " + (System.currentTimeMillis() - begin));
        System.out.println(lpgParser.getGraph().traversal().V().count().next());
        System.out.println(lpgParser.getGraph().traversal().E().count().next());
    }

    private static void testAsyncVersion() {
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;

        // 创建异步解析器，使用所有CPU核心
        AsyncLPGParser asyncParser = new AsyncLPGParser();

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
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", AsyncLPGParser.STRING),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", AsyncLPGParser.STRING),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", AsyncLPGParser.STRING, "creationDate", AsyncLPGParser.STRING),
                    asyncParser.asyncLoadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", AsyncLPGParser.STRING, "length", AsyncLPGParser.INT),
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
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", AsyncLPGParser.STRING),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place"),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", AsyncLPGParser.STRING),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", AsyncLPGParser.STRING),
                    asyncParser.asyncLoadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", AsyncLPGParser.STRING),
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

            System.out.println("load pg end (async) " + (System.currentTimeMillis() - begin));
            System.out.println(asyncParser.getGraph().traversal().V().count().next());
            System.out.println(asyncParser.getGraph().traversal().E().count().next());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            asyncParser.shutdown();
        }
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("LPGLoaderTest");
    }

}