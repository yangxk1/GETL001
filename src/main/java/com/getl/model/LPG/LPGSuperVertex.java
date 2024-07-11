package com.getl.model.LPG;

import cn.hutool.core.collection.CollectionUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.*;

/**
 * 多个节点组合的超级点，包含多个vertexes
 * labels 保存label，例如 government、
 * edges 保存浓缩边
 * groupBy 分组字段
 */
@Getter
@Setter
@Slf4j
public class LPGSuperVertex extends LPGVertex {
    //包含的vertexes
    private List<LPGElement> vertexes;
    //分组字段
    private LPGProperty groupBy;
    //是否build完成
    private boolean completed;

    public LPGSuperVertex(LPGGraph graph, String label) {
        super(graph,label);
        graph.addSuperVertex(this);
    }

    public Edge addInEdge(String label, String outVertexLabel, List<Edge> edges) {
        LPGElement outVertex = Optional.ofNullable(edges)
                .filter(CollectionUtil::isNotEmpty)
                .map(list -> list.get(0))
                .map(Edge::outVertex)
                .map(i -> {
                    if (getGraph().isSuperVertex(outVertexLabel)) {
                        return ((LPGElement) i).inSuperVertex(outVertexLabel);
                    }
                    return (LPGElement) i;
                })
                .orElse(null);
        if (outVertex == null) {
            log.info("super outVertex add edge error, has no in outVertex, label={},inVertexLabel={},edge={}", label, outVertexLabel, edges);
            return null;
        }
        LPGEdge lpgEdge = new LPGEdge(getGraph(), outVertex, this, label);
        this.addEdge(lpgEdge);
        lpgEdge.addPropertyValue("edge_step", edges);
        return lpgEdge;
    }

    public Edge addOutEdge(String label, String inVertexLabel, List<Edge> edges) {
        LPGElement inVertex = Optional.ofNullable(edges)
                .map(list -> list.get(list.size() - 1))
                .map(Edge::inVertex)
                .map(i -> {
                    if (getGraph().isSuperVertex(inVertexLabel)) {
                        return ((LPGElement) i).inSuperVertex(inVertexLabel);
                    }
                    return (LPGElement) i;
                })
                .orElse(null);
        if (inVertex == null) {
            log.info("super inVertex add edge error, has no in inVertex, label={},inVertexLabel={},edge={}", label, inVertexLabel, edges);
            return null;
        }
        LPGEdge lpgEdge = new LPGEdge(getGraph(), this, inVertex, label);
        this.addEdge(lpgEdge);
        lpgEdge.addPropertyValue("edge_step", edges);
        return lpgEdge;
    }

    public Iterator<Edge> subVertexEdges(Direction direction, String... edgeLabels) {
        Set<String> hashSet = new HashSet<>(List.of(edgeLabels));
        List<Edge> edges = new ArrayList<>();
        if (Direction.OUT.equals(direction) || Direction.BOTH.equals(direction)) {
            Optional.ofNullable(vertexes)
                    .orElse(Collections.emptyList())
                    .stream()
                    .flatMap(i -> i.getInEdges().stream())
                    .filter(i -> hashSet.contains(i.label()))
                    .map(i -> (Edge) i)
                    .forEach(edges::add);
        }
        if (Direction.IN.equals(direction) || Direction.BOTH.equals(direction)) {
            Optional.ofNullable(vertexes)
                    .orElse(Collections.emptyList())
                    .stream()
                    .flatMap(i -> i.getOutEdges().stream())
                    .filter(i -> hashSet.contains(i.label()))
                    .map(i -> (Edge) i)
                    .forEach(edges::add);
        }
        return edges.iterator();
    }

    public Iterator<Edge> superVertexEdges(Direction direction, String... edgeLabels) {
        return super.edges(direction, edgeLabels);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        if (completed) {
            return superVertexEdges(direction, edgeLabels);
        } else {
            return subVertexEdges(direction, edgeLabels);
        }
    }

}
