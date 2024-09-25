package com.getl.model.LPG;

import com.getl.model.ug.UnifiedGraph;
import com.getl.query.GraphTransaction;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;

import java.io.PrintWriter;
import java.util.*;

public class LPGGraph implements Graph {
    private volatile boolean closed = false;
    /**
     * The vertices of this labeled property graph.
     */
    @Setter
    private Map<Object, LPGVertex> vertices = new LinkedHashMap<>();

    public Map<Object, LPGVertex> getVs() {
        return vertices;
    }

    public Collection<LPGVertex> getVertices() {
        return vertices.values();
    }

    @Getter
    @Setter
    private Set<LPGSuperVertex> superVertices = new LinkedHashSet<>();
    @Getter
    @Setter
    private Set<String> superVertexLabels = new LinkedHashSet<>();
    /**
     * The edges of this labeled property graph.
     */
    @Getter
    @Setter
    private Set<LPGEdge> edges = new LinkedHashSet<>();

    /**
     * Adds a new vertex to this graph, if the vertex already exists in the graph nothing happens.
     *
     * @param v The vertex to add.
     */
    public void addVertex(@NonNull LPGVertex v) {
        if (v.id() == null) {
            v.setId(UnifiedGraph.getNextID());
        }
        vertices.put(v.getId(), v);
    }

    /**
     * Adds a new edge to this graph, if the edge already exists in the graph nothing happens.
     *
     * @param e The edge to add.
     */
    public void addEdge(@NonNull LPGEdge e) {
        edges.add(e);
    }

    public void addEdge(@NonNull Edge e) {
        edges.add((LPGEdge) e);
    }

    public void addVertices(@NonNull Vertex... vertices) {
        for (Vertex v : vertices) {
            this.addVertex((LPGVertex) v);
        }
    }

    /**
     * Adds new vertices to this graph, if a vertex already exists in the graph nothing happens.
     *
     * @param vertices The vertices to add.
     */
    public void addVertices(@NonNull LPGVertex... vertices) {
        for (LPGVertex v : vertices) {
            this.addVertex(v);
        }
    }

    /**
     * Adds new edges to this graph, if an edge already exists in the graph nothing happens.
     *
     * @param edges The edges to add.
     */
    public void addEdges(@NonNull LPGEdge... edges) {
        for (LPGEdge e : edges) {
            this.addEdge(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("---- Vertices ----\n");
        for (LPGVertex v : getVertices()) {
            str.append(v.toString()).append("\n");
            for (LPGProperty property : v.getProperties()) {
                str.append("properties: ").append(property).append("\n");
            }
//            str.append("\n");
        }
        str.append("---- Edges ----\n");
        for (LPGEdge e : edges) {
            str.append(e.toString()).append("\n");
            for (LPGProperty property : e.getProperties()) {
                str.append("properties: ").append(property).append("\n");
            }
//            str.append("\n");
        }
        return str.toString();
    }

    private GraphTransaction graphTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: graph operations must be committed manually,
         * Maybe users need to auto open tinkerpop tx by readWrite().
//         */
//        this.tx.readWrite();
//        return this.tx.graphTransaction();
        return new GraphTransaction(this);
    }

    private void checkGraphNotClosed() {
        assert !closed;
    }

    public void addSuperVertex(LPGSuperVertex LPGSuperVertex) {
        this.addVertex(LPGSuperVertex);
        this.superVertices.add(LPGSuperVertex);
        this.superVertexLabels.add(LPGSuperVertex.label());
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction().addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds.length == 0) {
            return this.graphTransaction().queryVertices();
        }
        return this.graphTransaction().queryVertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds.length == 0) {
            return this.graphTransaction().queryEdges();
        }
        return this.graphTransaction().queryEdges(edgeIds);
    }

    @Override
    public Transaction tx() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    public boolean isSuperVertex(String inVertexLabel) {
        return superVertexLabels.contains(inVertexLabel);
    }

    public void write(PrintWriter pw) {
        pw.println("---- Vertices ----");
        for (LPGVertex v : getVertices()) {
            StringBuilder str = new StringBuilder();
            str.append(v.toString()).append("\n");
            for (LPGProperty property : v.getProperties()) {
                str.append("properties: ").append(property).append("\n");
            }
            pw.println(str);
        }
        pw.println("---- Edges ----");
        for (LPGEdge e : edges) {
            StringBuilder str = new StringBuilder();
            str.append(e.toString()).append("\n");
            for (LPGProperty property : e.getProperties()) {
                str.append("properties: ").append(property).append("\n");
            }
            pw.println(str);
        }
    }

    public LPGVertex getVertex(Object id) {
        return vertices.get(id);
    }

    public LPGVertex getOrCreateVertex(Object id, String label, Iterator<VertexProperty<Object>> propertyIterator) {
        LPGVertex lpgVertex = vertices.get(id);
        if (lpgVertex == null) {
            lpgVertex = new LPGVertex(this, label);
            lpgVertex.setId(id);
            addVertex(lpgVertex);
            while (propertyIterator.hasNext()) {
                VertexProperty next = propertyIterator.next();
                lpgVertex.addPropertyValue(next.key(), next.value());
            }
        }
        return lpgVertex;
    }

    public LPGVertex getOrCreateVertex(Object id, String label) {
        return getOrCreateVertex(id, label, Collections.emptyIterator());
    }
}
