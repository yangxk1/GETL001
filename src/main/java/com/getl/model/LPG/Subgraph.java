package com.getl.model.LPG;

import com.getl.converter.LPGSuperVertexBuilder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of the formal definition of a subgraph.
 */
@Getter
@Setter
public class Subgraph extends LPGVertex {
    //Subgraph data
    private LPGGraph data;
    //property for grouping by
    private LPGProperty groupBy;
    private boolean completed;

    public void complete() {
        this.completed = true;
    }

    public Subgraph(LPGGraph graph, String label) {
        super(graph, label);
        data = new LPGGraph();
        graph.addVertex(this);
    }

    public void addVertex(Vertex lpgVertex) {
        data.addVertex((LPGVertex) lpgVertex);
    }

    public Iterator<Vertex> vertices(Object... vertexIds) {
        return data.vertices(vertexIds);
    }

    public Iterator<Edge> edges(Object... edgeIds) {
        return data.edges(edgeIds);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        if (completed) {
            return super.edges(direction, edgeLabels);
        }
        if (Direction.IN.equals(direction)) {
            return data.traversal().V().inE(edgeLabels);
        }
        return data.traversal().V().outE(edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        if (completed) {
            return super.vertices(direction, edgeLabels);
        }
        if (Direction.IN.equals(direction)) {
            return data.traversal().V().in(edgeLabels);
        }
        return data.traversal().V().out(edgeLabels);
    }

    public static class SubGraphBuilder {
        private static ExecutorService executorService;
        private LPGSuperVertexBuilder.GroupPop groupPop;
        private Object defaultSortValue = new Object();
        private String groupKey = "DEFAULT_GROUP_KEY";
        private String label;
        private LPGGraph graph;
        private List<Subgraph> subgraphs = new ArrayList<>();
        private Map<String, LPGSuperVertexBuilder.PopTrans> properties = new HashMap<>();

        public SubGraphBuilder(LPGGraph graph, String label) {
            this.graph = graph;
            this.label = label;
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

        public SubGraphBuilder traversal(List<Vertex> vertices) {
            Stream<Vertex> stream = Optional.ofNullable(vertices).orElse(Collections.emptyList()).stream();
            //分组
            Map<Object, List<LPGElement>> group = group(stream, groupKey);
            //create and set properties
            group.forEach((key, value) -> {
                Map<String, Object> pop = new HashMap<>();
                pop.put("super_vertex_size", value.size());
                properties.forEach((popName, method) -> pop.put(popName, method.compute(value)));
                Subgraph subGraph = new Subgraph(graph, label);
                subgraphs.add(subGraph);
                value.forEach(subGraph::addVertex);
                subGraph.addPropertyValue(groupKey, key);
                List<Object> subVertexIds = new ArrayList<>();
                value.forEach(vertex -> {
                    subVertexIds.add(vertex.id());
                });
                pop.put("sub_vertex_ids", subVertexIds);
                subGraph.addProperty(pop);
                graph.addVertex(subGraph);
            });
            return this;
        }

        public SubGraphBuilder addV(AddV function) {
            CountDownLatch countDownLatch = new CountDownLatch(subgraphs.size());
            ExecutorService executorPool = getExecutorPool();
            for (Subgraph subGraph : subgraphs) {
                executorPool.execute(() -> {
                    subGraph.vertices().forEachRemaining(vertex -> function.accept(subGraph, (LPGVertex) vertex));
                    countDownLatch.countDown();
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public void groupBy(String groupKey, LPGSuperVertexBuilder.GroupPop pop) {
            this.groupKey = groupKey;
            this.groupPop = pop;
        }

        public List<Subgraph> getSubGraphs() {
            return this.subgraphs;
        }

        public static ExecutorService getExecutorPool() {
            if (executorService == null) {
                executorService = Executors.newFixedThreadPool(128);
            }
            return executorService;
        }
    }

    public interface AddV {
        //        public void accept(SubGraph subGraph, Object vertexIds);
        public void accept(Subgraph subGraph, LPGVertex vertexIds);
    }
}