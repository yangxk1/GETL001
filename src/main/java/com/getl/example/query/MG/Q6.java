package com.getl.example.query.MG;

import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.converter.mg.PGMapperR4j;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

/**
 * MG 对照实验 Q6: 度数过滤 + 连通分量计算
 */
public class Q6 extends AbstractMGQuery {
    public static void main(String[] args) { new Q6().accept(); }

    private Set<Integer> computeComponents(LPGGraph lpgGraph) {
        List<LPGVertex> vertices = new ArrayList<>(lpgGraph.getVertices());
        int numVertices = vertices.size();
        List<Integer> rowOffsets = new ArrayList<>(vertices.size());
        List<Integer> columnIndices = new ArrayList<>(lpgGraph.getEdges().size());
        List<Integer> components = new ArrayList<>(numVertices);
        for (int i = 0; i < vertices.size(); i++) { Vertex vertex = vertices.get(i); vertex.property("_index", i); components.add(i); }
        int current = 0;
        for (int i = 0; i < vertices.size(); i++) { Vertex vertex = vertices.get(i); Iterator<Vertex> vertexIterator = vertex.vertices(org.apache.tinkerpop.gremlin.structure.Direction.BOTH); Set<Integer> neighbors = new HashSet<>(); while (vertexIterator.hasNext()) { neighbors.add((Integer) vertexIterator.next().property("_index").orElse(-1)); } rowOffsets.add(current); for (Integer neighbor : neighbors) { current++; columnIndices.add(neighbor); } }
        for (int v = 0; v < numVertices; v++) { compute(v, numVertices, rowOffsets, columnIndices, components); }
        for (int v = 0; v < numVertices; v++) { vertices.get(v).property("cc", components.get(v)); }
        return new HashSet<>(components);
    }

    private void compute(int v, int numVertices, List<Integer> rowOffsets, List<Integer> columnIndices, List<Integer> components) {
        int start = rowOffsets.get(v);
        int end = v + 1 < numVertices ? rowOffsets.get(v + 1) : columnIndices.size();
        for (int n = start; n < end; n++) {
            int neighbor = columnIndices.get(n);
            int min = components.get(v);
            if (min < components.get(neighbor)) { components.set(neighbor, min); compute(neighbor, numVertices, rowOffsets, columnIndices, components); }
        }
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN MG Q6");
            loadSourcePG();
            convertPGToMG();
            preparePGForQuery();
            LPGGraph lpgGraph = wrapAsLPG(pgGraphForQuery);
            long begin = System.currentTimeMillis();
            List<Map<String, Object>> results = lpgGraph.traversal().V()
                    .property("_degree", bothE().count())
                    .has("_degree", P.gt(20)).as("n1")
                    .outE().as("e1")
                    .inV().has("_degree", P.gt(20)).as("n2")
                    .select("n1", "e1", "n2")
                    .dedup("e1")
                    .toList();
            logger.debugInfo("degree >20 query end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            LPGGraph resultGraph = new LPGGraph();
            for (Map<String, Object> result : results) {
                Vertex n1 = (Vertex) result.get("n1"); Edge e1 = (Edge) result.get("e1"); Vertex n2 = (Vertex) result.get("n2");
                LPGVertex n1V = resultGraph.getOrCreateVertex(n1.id(), n1.label(), n1.properties());
                LPGVertex n2V = resultGraph.getOrCreateVertex(n2.id(), n2.label(), n2.properties());
                LPGEdge edge = new LPGEdge(resultGraph, n1V, n2V, e1.label()); edge.setId(e1.id());
            }
            logger.debugInfo("collect resultGraph end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            Set<Integer> cc = computeComponents(resultGraph);
            logger.debugInfo("compute cc end, cc size=" + cc.size(), (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            MGraph resultMG = new MGraph();
            new PGMapperR4j(resultMG).addPGToMG(resultGraph);
            this.mGraph = resultMG;
            logger.debugInfo("Result LPG -> MG end", (System.currentTimeMillis() - begin));
            exportMGToRDF();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override
    protected String getLoggerName() { return "MG_QUERY_6"; }
}
