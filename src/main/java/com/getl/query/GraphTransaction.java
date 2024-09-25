
package com.getl.query;

import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGProperty;
import com.getl.model.LPG.LPGVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;


public class GraphTransaction {

    private LPGGraph lpgGraph;
    private Map<Object, LPGVertex> addedVertices;
    private Map<Object, LPGVertex> removedVertices;

    private Map<Object, LPGEdge> addedEdges;
    private Map<Object, LPGEdge> removedEdges;

    private Set<LPGProperty> addedProps;
    private Set<LPGProperty> removedProps;

    // These are used to rollback state
    private Map<Object, LPGVertex> updatedVertices;
    private Map<Object, LPGEdge> updatedEdges;
    private Set<LPGProperty> updatedOldestProps;

    public GraphTransaction(LPGGraph lpgGraph) {
        this.lpgGraph = lpgGraph;
    }

    public LPGVertex addVertex(Object... keyValues) {
        if ((keyValues.length & 1) == 1) {
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        }
        Object id = null;
        List<String> label = new ArrayList<>();
        LPGVertex vertex = new LPGVertex(this.lpgGraph);
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            Object val = keyValues[i + 1];
            if (!(key instanceof String) && !(key instanceof T)) {
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }
            if (val == null) {
                // Ignore null value for tinkerpop test compatibility
                continue;
            }
            if (key.equals(T.id)) {
                id = val;
            } else if (key.equals(T.label)) {
                label.add(val.toString());
            } else {
                vertex.addPropertyValue(key.toString(), val);
            }
        }
        vertex.addLabel(label);
        if (id != null) {
            vertex.setId(id);
        }
        return this.addVertex(vertex);
    }

    public LPGVertex addVertex(LPGVertex vertex) {
        assert !vertex.isRemoved();
        lpgGraph.addVertex(vertex);
        return vertex;
    }

    public Iterator<Vertex> queryVertices() {
        return this.lpgGraph.getVertices().stream()
                .map(i -> (Vertex) i)
                .collect(Collectors.toList()).iterator();
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        Set<Object> vIds = Arrays.stream(vertexIds).collect(Collectors.toSet());
        Iterator iterator = Optional.ofNullable(vIds).stream().map(lpgGraph::getVertex).iterator();
        return iterator;
//        return this.lpgGraph.getVertices().stream()
////                .filter(i -> vIds.contains(i.id()))
////                .map(i -> (Vertex) i)
////                .collect(Collectors.toList()).iterator();
    }

    public Iterator<Edge> queryEdges() {
        return this.lpgGraph.getEdges().stream()
                .map(i -> (Edge) i)
                .collect(Collectors.toList()).iterator();
    }

    public Iterator<Edge> queryEdges(Object... edgeIds) {
        Set<Object> edges = Arrays.stream(edgeIds).collect(Collectors.toSet());
        return this.lpgGraph.getVertices().stream()
                .filter(i -> edges.contains(i.id()))
                .map(i -> (Edge) i)
                .collect(Collectors.toList()).iterator();
    }
}
