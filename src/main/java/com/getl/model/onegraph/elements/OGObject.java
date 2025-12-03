package com.getl.model.onegraph.elements;

import com.getl.model.onegraph.statements.OGTripleStatement;
/**
 * An {@link OGObject} is in {@code object} position of an {@link OGTripleStatement}.
 */
public interface OGObject {

    /**
     * Return {@code true} if this object could be the in-node of a labeled property graph edge,
     * false otherwise.
     * @return  {@code true} or {@code false} depending on if this object could be the in-node of a
     * labeled property graph edge.
     */
    boolean canBeInNodeOfEdge();
}
