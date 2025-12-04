package com.getl.example.converter;

import com.getl.constant.CommonConstant;
import com.getl.converter.ConverterUtils;
import com.getl.example.Runnable;
import com.getl.io.AsyncLPGParser;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Test for direct TinkerPop Graph to SG conversion without LPG intermediate layer
 * Verifies consistency between new direct mapper and old LPG-based mapper
 */
public class TinkerPopMapperTest extends Runnable {

    public static void main(String[] args) {
        new TinkerPopMapperTest().accept();
    }

    @Override
    protected void run() {
        System.out.println("=== TinkerPop Mapper 新旧方法一致性验证 ===\n");

        testTinkerPopToSGConsistency();
        System.out.println("\n" + "=".repeat(60) + "\n");

        testSGToTinkerPopConsistency();
        System.out.println("\n" + "=".repeat(60) + "\n");

        testRoundTripConsistency();
        System.out.println("\n" + "=".repeat(60) + "\n");

        testPerformanceComparison();
    }

    /**
     * 测试 TinkerPop → SG 转换的一致性
     */
    private void testTinkerPopToSGConsistency() {
        System.out.println("【测试1】TinkerPop → SG 转换一致性");
        System.out.println("-".repeat(60));

        Graph originalGraph = loadData();
        System.out.println("加载完成的图数据");

        // 旧方法：通过 LPG
        long oldStart = System.nanoTime();
        OGDataset datasetOld = ConverterUtils.buildSGFromTinkerPopGraphWithLPG(originalGraph);
        long oldTime = System.nanoTime() - oldStart;

        // 新方法：直接转换
        long newStart = System.nanoTime();
        OGDataset datasetNew = ConverterUtils.buildSGFromTinkerPopGraph(originalGraph);
        long newTime = System.nanoTime() - newStart;


        // 验证结果一致性
        boolean consistent = verifyOGDatasetConsistency(datasetNew, datasetOld);

        System.out.println("\n结果统计:");
        System.out.printf("  新方法 (直接转换):%n");
        System.out.printf("    - 耗时: %.3f ms%n", newTime / 1_000_000.0);
        System.out.printf("    - Property Statements: %d%n", countStatements(datasetNew.getPropertyStatements()));
        System.out.printf("    - Relationship Statements: %d%n", countStatements(datasetNew.getRelationshipStatements()));

        System.out.printf("  旧方法 (经过LPG):%n");
        System.out.printf("    - 耗时: %.3f ms%n", oldTime / 1_000_000.0);
        System.out.printf("    - Property Statements: %d%n", countStatements(datasetOld.getPropertyStatements()));
        System.out.printf("    - Relationship Statements: %d%n", countStatements(datasetOld.getRelationshipStatements()));

        System.out.printf("%n  性能提升: %.2fx%n", (double) oldTime / newTime);
        System.out.printf("  数据一致性: %s %s%n",
            consistent ? "✅" : "❌",
            consistent ? "通过" : "失败");

        logger.debugInfo("TinkerPop→SG 新方法", newTime / 1_000_000);
        logger.debugInfo("TinkerPop→SG 旧方法", oldTime / 1_000_000);
        logger.debugInfo("TinkerPop→SG 一致性", consistent ? 1 : 0);
    }

    /**
     * 测试 SG → TinkerPop 转换的一致性
     */
    private void testSGToTinkerPopConsistency() {
        System.out.println("【测试2】SG → TinkerPop 转换一致性");
        System.out.println("-".repeat(60));

        // 先创建一个 OGDataset
        Graph sourceGraph = createSampleTinkerGraph();
        OGDataset dataset = ConverterUtils.buildSGFromTinkerPopGraph(sourceGraph);

        // 新方法：直接转换
        long newStart = System.nanoTime();
        Graph graphNew = ConverterUtils.buildTinkerPopGraphFromSG(dataset);
        long newTime = System.nanoTime() - newStart;

        // 旧方法：通过 LPG
        long oldStart = System.nanoTime();
        Graph graphOld = ConverterUtils.buildTinkerPopGraphFromSGWithLPG(dataset);
        long oldTime = System.nanoTime() - oldStart;

        // 验证结果一致性
        boolean consistent = verifyGraphConsistency(graphNew, graphOld);

        GraphTraversalSource gNew = graphNew.traversal();
        GraphTraversalSource gOld = graphOld.traversal();

        System.out.println("\n结果统计:");
        System.out.printf("  新方法 (直接转换):%n");
        System.out.printf("    - 耗时: %.3f ms%n", newTime / 1_000_000.0);
        System.out.printf("    - 顶点数: %d%n", gNew.V().count().next());
        System.out.printf("    - 边数: %d%n", gNew.E().count().next());

        System.out.printf("  旧方法 (经过LPG):%n");
        System.out.printf("    - 耗时: %.3f ms%n", oldTime / 1_000_000.0);
        System.out.printf("    - 顶点数: %d%n", gOld.V().count().next());
        System.out.printf("    - 边数: %d%n", gOld.E().count().next());

        System.out.printf("%n  性能提升: %.2fx%n", (double) oldTime / newTime);
        System.out.printf("  数据一致性: %s %s%n",
            consistent ? "✅" : "❌",
            consistent ? "通过" : "失败");

        logger.debugInfo("SG→TinkerPop 新方法", newTime / 1_000_000);
        logger.debugInfo("SG→TinkerPop 旧方法", oldTime / 1_000_000);
        logger.debugInfo("SG→TinkerPop 一致性", consistent ? 1 : 0);
    }

    /**
     * 测试往返转换的一致性
     */
    private void testRoundTripConsistency() {
        System.out.println("【测试3】往返转换一致性验证");
        System.out.println("-".repeat(60));

        Graph originalGraph = loadData();
        GraphTraversalSource gOrig = originalGraph.traversal();
        long origV = gOrig.V().count().next();
        long origE = gOrig.E().count().next();

        System.out.println("原始图:");
        System.out.printf("  顶点数: %d%n", origV);
        System.out.printf("  边数: %d%n", origE);

        // 新方法往返
        long newStart = System.nanoTime();
        OGDataset datasetNew = ConverterUtils.buildSGFromTinkerPopGraph(originalGraph);
        Graph graphNew = ConverterUtils.buildTinkerPopGraphFromSG(datasetNew);
        long newTime = System.nanoTime() - newStart;

        // 旧方法往返
        long oldStart = System.nanoTime();
        OGDataset datasetOld = ConverterUtils.buildSGFromTinkerPopGraphWithLPG(originalGraph);
        Graph graphOld = ConverterUtils.buildTinkerPopGraphFromSGWithLPG(datasetOld);
        long oldTime = System.nanoTime() - oldStart;

        // 验证结果
        GraphTraversalSource gNew = graphNew.traversal();
        GraphTraversalSource gOld = graphOld.traversal();

        long newV = gNew.V().count().next();
        long newE = gNew.E().count().next();
        long oldV = gOld.V().count().next();
        long oldE = gOld.E().count().next();

        boolean newPreserved = (newV == origV && newE == origE);
        boolean oldPreserved = (oldV == origV && oldE == origE);
        boolean consistent = verifyGraphConsistency(graphNew, graphOld);

        System.out.println("\n新方法往返结果:");
        System.out.printf("  耗时: %.3f ms%n", newTime / 1_000_000.0);
        System.out.printf("  顶点数: %d (原始: %d) %s%n", newV, origV, newPreserved ? "✅" : "❌");
        System.out.printf("  边数: %d (原始: %d) %s%n", newE, origE, newPreserved ? "✅" : "❌");

        System.out.println("\n旧方法往返结果:");
        System.out.printf("  耗时: %.3f ms%n", oldTime / 1_000_000.0);
        System.out.printf("  顶点数: %d (原始: %d) %s%n", oldV, origV, oldPreserved ? "✅" : "❌");
        System.out.printf("  边数: %d (原始: %d) %s%n", oldE, origE, oldPreserved ? "✅" : "❌");

        System.out.printf("%n  性能提升: %.2fx%n", (double) oldTime / newTime);
        System.out.printf("  新旧方法一致性: %s %s%n",
            consistent ? "✅" : "❌",
            consistent ? "通过" : "失败");
        System.out.printf("  数据完整性: %s %s%n",
            (newPreserved && oldPreserved) ? "✅" : "❌",
            (newPreserved && oldPreserved) ? "通过" : "失败");

        logger.debugInfo("往返转换 新方法", newTime / 1_000_000);
        logger.debugInfo("往返转换 旧方法", oldTime / 1_000_000);
        logger.debugInfo("往返转换 一致性", consistent ? 1 : 0);
    }

    /**
     * 性能对比测试
     */
    private void testPerformanceComparison() {
        System.out.println("【测试4】性能对比测试 (10次迭代)");
        System.out.println("-".repeat(60));

        int iterations = 10;
        long totalNewTime = 0;
        long totalOldTime = 0;

        for (int i = 0; i < iterations; i++) {
            Graph graph = createSampleTinkerGraph();

            // 新方法
            long newStart = System.nanoTime();
            OGDataset datasetNew = ConverterUtils.buildSGFromTinkerPopGraph(graph);
            Graph resultNew = ConverterUtils.buildTinkerPopGraphFromSG(datasetNew);
            totalNewTime += (System.nanoTime() - newStart);

            // 旧方法
            long oldStart = System.nanoTime();
            OGDataset datasetOld = ConverterUtils.buildSGFromTinkerPopGraphWithLPG(graph);
            Graph resultOld = ConverterUtils.buildTinkerPopGraphFromSGWithLPG(datasetOld);
            totalOldTime += (System.nanoTime() - oldStart);
        }

        double avgNew = totalNewTime / (double) iterations / 1_000_000.0;
        double avgOld = totalOldTime / (double) iterations / 1_000_000.0;
        double speedup = (double) totalOldTime / totalNewTime;

        System.out.printf("%n平均性能 (%d 次迭代):%n", iterations);
        System.out.printf("  新方法 (直接转换): %.3f ms%n", avgNew);
        System.out.printf("  旧方法 (经过LPG):  %.3f ms%n", avgOld);
        System.out.printf("  性能提升: %.2fx 🚀%n", speedup);
        System.out.printf("  时间节省: %.1f%%%n", (1 - 1/speedup) * 100);

        logger.debugInfo("性能对比 新方法平均", (long) avgNew);
        logger.debugInfo("性能对比 旧方法平均", (long) avgOld);
        logger.debugInfo("性能对比 加速比", (long) (speedup * 100));
    }

    /**
     * 验证两个 OGDataset 的一致性
     */
    private boolean verifyOGDatasetConsistency(OGDataset dataset1, OGDataset dataset2) {
        int propCount1 = countStatements(dataset1.getPropertyStatements());
        int propCount2 = countStatements(dataset2.getPropertyStatements());
        int relCount1 = countStatements(dataset1.getRelationshipStatements());
        int relCount2 = countStatements(dataset2.getRelationshipStatements());

        return (propCount1 == propCount2) && (relCount1 == relCount2);
    }

    /**
     * 验证两个 TinkerPop Graph 的一致性
     */
    private boolean verifyGraphConsistency(Graph graph1, Graph graph2) {
        GraphTraversalSource g1 = graph1.traversal();
        GraphTraversalSource g2 = graph2.traversal();

        long v1 = g1.V().count().next();
        long v2 = g2.V().count().next();
        long e1 = g1.E().count().next();
        long e2 = g2.E().count().next();

        return (v1 == v2) && (e1 == e2);
    }

    /**
     * 创建测试用的 TinkerPop 图
     */
    private Graph createSampleTinkerGraph() {
        TinkerGraph graph = TinkerGraph.open();

        // 创建顶点
        Vertex person1 = graph.addVertex("Person");
        person1.property("name", "Alice");
        person1.property("age", 30);
        person1.property("email", "alice@example.com");

        Vertex person2 = graph.addVertex("Person");
        person2.property("name", "Bob");
        person2.property("age", 25);
        person2.property("email", "bob@example.com");

        Vertex company = graph.addVertex("Company");
        company.property("name", "TechCorp");
        company.property("founded", 2010);
        company.property("industry", "Technology");

        Vertex project = graph.addVertex("Project");
        project.property("name", "OneGraph");
        project.property("status", "Active");

        // 创建边
        Edge knows = person1.addEdge("knows", person2);
        knows.property("since", 2015);
        knows.property("relation", "friend");

        Edge worksAt1 = person1.addEdge("worksAt", company);
        worksAt1.property("position", "Engineer");
        worksAt1.property("startYear", 2018);

        Edge worksAt2 = person2.addEdge("worksAt", company);
        worksAt2.property("position", "Designer");
        worksAt2.property("startYear", 2020);

        Edge workOn1 = person1.addEdge("worksOn", project);
        workOn1.property("role", "Lead Developer");

        Edge workOn2 = person2.addEdge("worksOn", project);
        workOn2.property("role", "UI Designer");

        return graph;
    }

    /**
     * 统计语句数量
     */
    private int countStatements(Iterable<?> iterable) {
        int count = 0;
        for (Object ignored : iterable) {
            count++;
        }
        return count;
    }

    @Override
    protected String loggerName() {
        return "TinkerPopMapperTest";
    }

    private Graph loadData() {
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
            return asyncParser.getGraph();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        } finally {
            // 关键修复：必须关闭 AsyncLPGParser 以终止后台线程
            asyncParser.shutdown();
        }
    }
}
