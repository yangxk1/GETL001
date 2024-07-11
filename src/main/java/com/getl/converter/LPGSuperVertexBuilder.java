package com.getl.converter;

import com.getl.Graph;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGElement;
import com.getl.model.LPG.LPGSuperVertex;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LPGSuperVertexBuilder {
    private Graph graph;
    private String label;
    private Map<String, PopTrans> properties = new HashMap<>();
    private String groupKey = "DEFAULT_GROUP_KEY";
    private GroupPop groupPop;
    private Object defaultSortValue = new Object();
    private List<LPGSuperVertex> superVertices = new ArrayList<>();
    private boolean loop;

    public static LPGSuperVertexBuilder getLPGSuperVertexBuilder(Graph graph) {
        return new LPGSuperVertexBuilder(graph);
    }

    public LPGSuperVertexBuilder(Graph graph) {
        this.graph = graph;
    }

    public LPGSuperVertexBuilder label(String label) {
        this.label = label;
        return this;
    }

    public LPGSuperVertexBuilder addProperties(String propertiesName, PopTrans transMethod) {
        properties.put(propertiesName, transMethod);
        return this;
    }

    public LPGSuperVertexBuilder groupBy(String groupBy, GroupPop groupPop) {
        this.groupKey = groupBy;
        this.groupPop = groupPop;
        return this;
    }

    public Map<Object, List<LPGElement>> group(Stream<Vertex> vertexStream, String groupKey) {
        if (StringUtils.isBlank(groupKey) || "DEFAULT_GROUP_KEY".equals(groupKey)) {
            HashMap<Object, List<LPGElement>> listHashMap = new HashMap<>();
            listHashMap.put(defaultSortValue, vertexStream.map(i -> (LPGElement) i).collect(Collectors.toList()));
            return listHashMap;
        }
        return vertexStream
                .map(i -> (LPGElement) i)
                .collect(Collectors.groupingBy(
                        vertex -> groupPop.compute(vertex).orElse(defaultSortValue)));
    }

    public LPGSuperVertexBuilder traversal(List<Vertex> vertices) {
        Stream<Vertex> stream = Optional.ofNullable(vertices).orElse(Collections.emptyList()).stream();
        Map<Object, List<LPGElement>> group = group(stream, groupKey);
        //create and set properties
        group.forEach((key, value) -> {
            Map<String, Object> pop = new HashMap<>();
            pop.put("super_vertex_size", value.size());
            properties.forEach((popName, method) -> pop.put(popName, method.compute(value)));
            LPGSuperVertex LPGSuperVertex = new LPGSuperVertex(graph.getLpgGraph(), label);
            LPGSuperVertex.setVertexes(value);
            LPGSuperVertex.addPropertyValue(groupKey, key);
            List<Object> subVertexIds = new ArrayList<>();
            value.forEach(vertex -> {
                subVertexIds.add(vertex.id());
                vertex.getSuperVertexLabels().add(label);
                vertex.getSuperVertexes().put(label, LPGSuperVertex);
            });
            pop.put("sub_vertex_ids", subVertexIds);
            LPGSuperVertex.addProperty(pop);
            graph.getLpgGraph().addVertex(LPGSuperVertex);
            superVertices.add(LPGSuperVertex);
        });
        return this;
    }

    public LPGSuperVertexBuilder addEdge(List<Path> paths, String label, Direction direction, String beginLabel, String lastLabel) {
        Optional.ofNullable(paths).orElse(Collections.emptyList()).stream().filter(Objects::nonNull).forEach(i -> addEdge(i, label, direction, beginLabel, lastLabel));
        return this;
    }

    public LPGSuperVertexBuilder addEdge(Path path, String label, Direction direction, String beginLabel, String lastLabel) {
        LPGElement beg = path.get(0);
        LPGElement end = path.get(path.size() - 1);
        beg = graph.getLpgGraph().isSuperVertex(beginLabel) ? beg.inSuperVertex(beginLabel) : beg;
        end = graph.getLpgGraph().isSuperVertex(beginLabel) ? end.inSuperVertex(lastLabel) : end;
        if (beg == null || end == null || beg == end) {
            return this;
        }
        if (Direction.BOTH.equals(direction) || Direction.IN.equals(direction)) {
            LPGEdge edge = new LPGEdge(this.graph.getLpgGraph(), beg, end, label);
            edge.addPropertyValue("path", path);
            graph.getLpgGraph().addEdges(edge);
        }
        if (Direction.BOTH.equals(direction) || Direction.OUT.equals(direction)) {
            LPGEdge edge = new LPGEdge(this.graph.getLpgGraph(), end, beg, label);
            edge.addPropertyValue("path", path);
            graph.getLpgGraph().addEdges(edge);
        }
        return this;
    }

    public void addEdge(LPGSuperVertex LPGSuperVertex, String label, Direction direction, String otherVertexLabel, List<Edge> edges) {
        switch (direction) {
            case IN:
                LPGSuperVertex.addInEdge(label, otherVertexLabel, edges);
                break;
            case OUT:
                LPGSuperVertex.addOutEdge(label, otherVertexLabel, edges);
                break;
            default:
        }
    }

    public List<LPGSuperVertex> finish() {
        superVertices.forEach(i -> i.setCompleted(true));
        return superVertices;
    }

    public LPGSuperVertexBuilder loop(boolean loop) {
        this.loop = loop;
        return this;
    }

    @Data
    public class InnerVertex {
        private LPGElement lpgElement;
        private Map<String, Object> pop;

        public InnerVertex(LPGElement lpgElement) {
            this.lpgElement = lpgElement;
            pop = new HashMap<>();
        }
    }

    public interface PopTrans {
        public Object compute(List<LPGElement> lpgElements);
    }

    public interface GroupPop {
        public Optional<Object> compute(LPGElement lpgElement);
    }
}
