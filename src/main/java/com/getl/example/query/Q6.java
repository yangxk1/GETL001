package com.getl.example.query;

import com.getl.Graph;
import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class Q6 extends Runnable {
    public static void main(String[] args) {
        new Q6().accept();
    }

    public static Set<Integer> computeComponents(LPGGraph lpgGraph) {
        List<LPGVertex> vertices = new ArrayList<>(lpgGraph.getVertices());
        int numVertices = vertices.size();
        List<Integer> rowOffsets = new ArrayList<>(vertices.size());
        List<Integer> columnIndices = new ArrayList<>(lpgGraph.getEdges().size());
        List<Integer> components = new ArrayList<>(numVertices);
        for (int i = 0; i < vertices.size(); i++) {
            Vertex vertex = vertices.get(i);
            vertex.property("_index", i);
            components.add(i);
        }
        int current = 0;
        for (int i = 0; i < vertices.size(); i++) {
            Vertex vertex = vertices.get(i);
            Iterator<Vertex> vertexIterator = vertex.vertices(Direction.BOTH);
            Set<Integer> neighbors = new HashSet<>();
            while (vertexIterator.hasNext()) {
                neighbors.add((Integer) vertexIterator.next().property("_index").orElse(-1));
            }
            rowOffsets.add(current);
            for (Integer neighbor : neighbors) {
                current++;
                columnIndices.add(neighbor);
            }
        }
        for (int v = 0; v < numVertices; v++) {
            compute(v, numVertices, rowOffsets, columnIndices, components);
        }
        for (int v = 0; v < numVertices; v++) {
            vertices.get(v).property("cc", components.get(v));
        }
        HashSet<Integer> set = new HashSet<>(components);
        System.out.println("cc count: " + set.size());
        return set;
    }

    public static void compute(int v, int numVertices, List<Integer> rowOffsets, List<Integer> columnIndices, List<Integer> components) {
        int start = rowOffsets.get(v);
        int end = v + 1 < numVertices ? rowOffsets.get(v + 1) : columnIndices.size();
        for (int n = start; n < end; n++) {
            int neighbor = columnIndices.get(n);
            int min = components.get(v);
            if (min < components.get(neighbor)) {
                components.set(neighbor, min);
                compute(neighbor, numVertices, rowOffsets, columnIndices, components);
            }
        }
    }

    @Override
    public void accept() {
        try {
            UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRDFFile();
            long begin = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            GraphAPI graphAPI = GraphAPI.open();
            graphAPI.setUGMGraph(unifiedGraph);
            graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
            graphAPI.refreshLPG();
            LPGGraph lpgGraph = graphAPI.getGraph().getLpgGraph();
            graphAPI.setGraph(null);
            unifiedGraph = null;
            graphAPI = null;
            DebugUtil.DebugInfo("UGM2LPG end " + (System.currentTimeMillis() - begin));
            System.out.println("result vertex count: " + lpgGraph.getVertices().size());
            System.out.println("result edge count: " + lpgGraph.getEdges().size());
            begin = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            GraphTraversalSource g = lpgGraph.traversal();// Initialize your GraphTraversalSource
// Q3: Select vertices with a degree greater than 20 and the edges between them
            List<Map<String, Object>> results = g.V()
                    .property("_degree", bothE().count()) // 计算度数并存储在顶点属性中
                    .has("_degree", P.gt(20)).as("n1")
                    .outE().as("e1")
                    .inV().has("_degree", P.gt(20)).as("n2")
                    .select("n1", "e1", "n2")
                    .dedup("e1") // 移除重复的边
                    .toList();
            DebugUtil.DebugInfo("query (degree > 20) end " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            LPGGraph resultGraph = new LPGGraph();
            for (Map<String, Object> result : results) {
                Vertex n1 = (LPGVertex) result.get("n1");
                Edge e1 = (LPGEdge) result.get("e1");
                Vertex n2 = (LPGVertex) result.get("n2");
                LPGVertex n1V = resultGraph.getOrCreateVertex(n1.id(), n1.label(), n1.properties());
                LPGVertex n2V = resultGraph.getOrCreateVertex(n2.id(), n2.label(), n2.properties());
                LPGEdge lpgEdge = new LPGEdge(resultGraph, n1V, n2V, e1.label());
                lpgEdge.setId(e1.id());
            }
            DebugUtil.DebugInfo("collect to lpg end " + (System.currentTimeMillis() - begin));
            System.out.println("vertices count: " + resultGraph.getVertices().size());
            System.out.println("edge count: " + resultGraph.getEdges().size());
            begin = System.currentTimeMillis();
            computeComponents(resultGraph);
            DebugUtil.DebugInfo("cc end " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            Runtime.getRuntime().gc();
            DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            unifiedGraph = (new TinkerPopConverter(null, resultGraph, new HashMap<>())).createUGMFromTinkerPopGraph();
            DebugUtil.DebugInfo("lpg result 2 UGM end " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            RMGraph rmGraph = new RMGraph();
            rmGraph.addSchema(new Schema("IRI").addColumn("origin_label", Schema.MID_TEXT).addColumn("cc", Schema.INT));
            rmGraph.addSchema(new Schema("predicate", "IRI1", "IRI2"));
            RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
            rmConverter.addUGMToRMModel();
            DebugUtil.DebugInfo("UGM 2 RM end " + (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            MysqlSessions sessions = new MysqlSessions(CommonConstant.RESULT_JDBC_URL_6, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
            MysqlOp.createSchema(sessions);
            MysqlOp.write(sessions, rmGraph);
            DebugUtil.DebugInfo("Write RM end " + (System.currentTimeMillis() - begin));
            System.out.println("Lines: " + rmGraph.getLines().size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}