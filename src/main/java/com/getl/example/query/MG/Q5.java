package com.getl.example.query.MG;

import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.Subgraph;
import com.getl.query.step.MultiLabelP;
import com.getl.model.MG.MGraph;
import com.getl.converter.mg.PGMapperR4j;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.apache.tinkerpop.gremlin.structure.T.label;

/**
 * MG 对照实验 Q5: 基于标签的子图聚类 + 统计共享用户
 */
public class Q5 extends AbstractMGQuery {
    public static void main(String[] args) {
        new Q5().accept();
    }

    private void subGraph(LPGGraph graph) {
        Subgraph.SubGraphBuilder subGraphBuilder = new Subgraph.SubGraphBuilder(graph, "classify_by_tag");
        subGraphBuilder.groupBy("tag", movie -> {
            Iterator<Edge> edges = movie.edges(Direction.BOTH, "genome-scores");
            double relevance = -5;
            String tag = null;
            while (edges.hasNext()) {
                Edge next = edges.next();
                Object o = next.property("relevance").orElse(-5);
                if (relevance < (double) o) {
                    tag = next.inVertex().id().toString();
                    relevance = (double) o;
                }
            }
            return Optional.ofNullable(tag);
        });
        subGraphBuilder.traversal(graph.traversal().V().has(label, "movie").toList());
        List<Subgraph> subgraphs = subGraphBuilder.getSubGraphs();
        logger.debugInfo("group end subGraph number: " + subgraphs.size());
        subGraphBuilder.addV(((subgraph, vertex) -> {
            vertex.vertices(Direction.BOTH, "genome-scores").forEachRemaining(subgraph::addVertex);
            vertex.edges(Direction.BOTH, "ratings").forEachRemaining(edge -> {
                Object o = edge.property("rating").orElse(0);
                if (5 <= (double) o) {
                    subgraph.addVertex(edge.outVertex());
                }
            });
        }));
        logger.debugInfo("addV end");
        Map<Object, Set<Object>> userInSubGraph = new ConcurrentHashMap<>();
        ExecutorService executorPool = Subgraph.SubGraphBuilder.getExecutorPool();
        CountDownLatch countDownLatch = new CountDownLatch(subgraphs.size());
        for (Subgraph subGraph : subgraphs) {
            executorPool.execute(() -> {
                Set<Object> objects = userInSubGraph.get(subGraph.id());
                if (objects == null) {
                    Set<Object> user = subGraph.getData().traversal().V().has(label, MultiLabelP.of("user")).id().toSet();
                    userInSubGraph.put(subGraph.id(), user);
                }
                countDownLatch.countDown();
            });
        }
        try { countDownLatch.await(); } catch (InterruptedException e) { throw new RuntimeException(e); }
        logger.debugInfo("agg user end");
        for (int i = 0; i < subgraphs.size() - 1; i++) {
            Subgraph a = subgraphs.get(i); Set<Object> u1 = userInSubGraph.get(a.id()); if (u1 == null || u1.isEmpty()) { continue; }
            for (int j = i + 1; j < subgraphs.size(); j++) { Subgraph b = subgraphs.get(j); Set<Object> u2 = userInSubGraph.get(b.id()); if (u2 == null || u2.isEmpty()) { continue; }
                HashSet<Object> resSet = new HashSet<>(u1); resSet.retainAll(u2); if (resSet.isEmpty()) { continue; }
                Edge edge = a.addEdge("have_same_user", b); edge.property("count", resSet.size()); edge = b.addEdge("same_user_count", a); edge.property("count", resSet.size()); }
        }
        subgraphs.forEach(Subgraph::complete);
        logger.debugInfo("addE end");
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN MG Q5");
            loadSourcePG();
            convertPGToMG();
            preparePGForQuery();
            LPGGraph lpgGraph = wrapAsLPG(pgGraphForQuery);
            subGraph(lpgGraph);
            MGraph resultMG = new MGraph();
            new PGMapperR4j(resultMG).addPGToMG(lpgGraph);
            this.mGraph = resultMG;
            exportMGToRDF();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override
    protected String getLoggerName() { return "MG_QUERY_5"; }
}
