package com.getl.model.LPG;

import lombok.Getter;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class LPGEdge extends LPGElement implements Edge {
    /**
     * The label of this edge
     */
    public final String label;
    /**
     * The out-vertex of this edge
     */
    @Getter
    public final LPGElement outVertex;
    /**
     * The in-vertex of this edge
     */
    @Getter
    public final LPGElement inVertex;

    /**
     * Creates a new edge
     *
     * @param outVertex The out-vertex
     * @param inVertex  The in-vertex
     * @param label     The label of this edge
     */
    public LPGEdge(LPGGraph lpgGraph, @NonNull LPGElement outVertex, @NonNull LPGElement inVertex, @NonNull String label) {
        super(lpgGraph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.label = label;
        outVertex.addEdge(this);
        inVertex.addEdge(this);
        lpgGraph.addEdges(this);
    }

    @Override
    public String toString() {
        return this.getId() + " - " + this.outVertex + "-(" + this.label + ")-" + this.inVertex;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (Direction.IN.equals(direction)) {
            return Collections.singletonList((Vertex) this.getInVertex()).iterator();
        } else if (Direction.OUT.equals(direction)) {
            return Collections.singletonList((Vertex) this.getOutVertex()).iterator();
        }
        ArrayList<Vertex> vertices = new ArrayList<>();
        vertices.add(this.inVertex());
        vertices.add(this.outVertex());
        return vertices.iterator();
    }

    @Override
    public Iterator<LPGProperty> properties(final String... propertyKeys){
        return super.properties(propertyKeys);
    }
}
