package com.getl.example.query;

import cn.hutool.core.collection.CollectionUtil;
import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.RMConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.Subgraph;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.query.step.MultiLabelP;
import com.getl.util.DebugUtil;
import com.mysql.cj.util.LogUtils;
import org.apache.tinkerpop.gremlin.structure.*;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.apache.tinkerpop.gremlin.structure.T.label;

public class Q5 extends Runnable {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        DebugUtil.DebugInfo("BEGIN TO TEST Q5");
        UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRMDataset();
        long begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
        graphAPI.refreshLPG();
        LPGGraph lpgGraph = graphAPI.getGraph().getLpgGraph();
        DebugUtil.DebugInfo("UGM2LPG end " + (System.currentTimeMillis() - begin));
        System.out.println("vertex size :" + lpgGraph.getVertices().size());
        System.out.println("edge size :" + lpgGraph.getEdges().size());
        begin = System.currentTimeMillis();
        subGraph(lpgGraph);
        DebugUtil.DebugInfo("SUBGRAPH end" + (System.currentTimeMillis() - begin));
        System.out.println("add Edge count:" + lpgGraph.traversal().E().has(label, MultiLabelP.of("same_user_count")).toList().size());
        begin = System.currentTimeMillis();
        LPGGraph resultGraph = new LPGGraph();
        lpgGraph.traversal().V().has(label, MultiLabelP.of("classify_by_tag")).forEachRemaining(resultGraph::addVertices);
        lpgGraph.traversal().E().has(label, MultiLabelP.of("same_user_count")).forEachRemaining(resultGraph::addEdge);
        DebugUtil.DebugInfo("collect result" + (System.currentTimeMillis() - begin));
        unifiedGraph = (new LPGGraphConverter(null, resultGraph, new HashMap<>())).createUGMFromLPGGraph();
        DebugUtil.DebugInfo("lpg 2 UGM end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
        graphAPI.refreshLPG();
        lpgGraph = graphAPI.getGraph().getLpgGraph();
        DebugUtil.DebugInfo("result UGM 2 LPG end " + (System.currentTimeMillis() - begin));
        System.out.println("lpg vertex count: " + lpgGraph.getVertices().size());
        System.out.println("lpg edge count: " + lpgGraph.getEdges().size());
        Subgraph.SubGraphBuilder.getExecutorPool().shutdown();
    }

    private static void subGraph(LPGGraph graph) {
        Subgraph.SubGraphBuilder subGraphBuilder = new Subgraph.SubGraphBuilder(graph, "classify_by_tag");
        subGraphBuilder.traversal(graph.traversal().V().has(label, "movie").toList());
        subGraphBuilder.groupBy("tag", movie -> {
            Iterator<Edge> edges = movie.edges(Direction.OUT, "genome-scores");
            double relevance = -5;
            String tag = null;
            while (edges.hasNext()) {
                Edge next = edges.next();
                Object o = next.property("relevance").orElse(-5);
                if (relevance < (double) o) {
                    tag = next.inVertex().id().toString();
                }
            }
            return Optional.ofNullable(tag);
        });
        subGraphBuilder.traversal(graph.traversal().V().has(label, "movie").toList());
        List<Subgraph> subgraphs = subGraphBuilder.getSubGraphs();
        DebugUtil.DebugInfo("group end subGraph number: " + subgraphs.size());
        subGraphBuilder.addV(((subgraph, vertex) -> {
            vertex.vertices(Direction.OUT, "genome-scores").forEachRemaining(subgraph::addVertex);
            vertex.edges(Direction.IN, "ratings").forEachRemaining(edge -> {
                Object o = edge.property("rating").orElse(0);
                if (5 <= (double) o) {
                    subgraph.addVertex(edge.outVertex());
                }
            });
//            graph.traversal().V(vertex.id()).inE("ratings").has("rating", P.gte(5)).outV().toList().forEach(subGraph::addVertex);
        }));
        DebugUtil.DebugInfo("addV end ");
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
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        DebugUtil.DebugInfo("agg user end: ");
        for (int i = 0; i < subgraphs.size() - 1; i++) {
//            System.out.println("No." + i + " : " + System.currentTimeMillis());
            Subgraph a = subgraphs.get(i);
            Set<Object> u1 = userInSubGraph.get(a.id());
            if (CollectionUtil.isEmpty(u1)) {
                continue;
            }
            for (int j = i + 1; j < subgraphs.size(); j++) {
                Subgraph b = subgraphs.get(j);
                Set<Object> u2 = userInSubGraph.get(b.id());
                if (CollectionUtil.isEmpty(u2)) {
                    continue;
                }
                HashSet<Object> resSet = new HashSet<>(u1);
                resSet.retainAll(u2);
                if (resSet.isEmpty()) {
                    continue;
                }
                Edge edge = a.addEdge("have_same_user", b);
                edge.property("count", resSet.size());
                edge = b.addEdge("same_user_count", a);
                edge.property("count", resSet.size());
            }
        }
        subgraphs.forEach(Subgraph::complete);
        DebugUtil.DebugInfo("addE end: ");
    }

    @Override
    public String init() {
        return validateParams(CommonConstant.JDBC_PASSWORD, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_URL);
    }

    @Override
    public void forward() {
        try {
            main(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
