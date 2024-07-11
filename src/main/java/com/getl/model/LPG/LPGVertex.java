package com.getl.model.LPG;

import cn.hutool.core.collection.CollectionUtil;
import lombok.NonNull;

import java.util.*;

/**
 * Simple Vertex in LPGGraph
 */
public class LPGVertex extends LPGElement {

    /**
     * Vertices can have multiple labels, but labels can't be duplicate.
     */
    private final LinkedHashSet<String> labels = new LinkedHashSet<>();

    /**
     * Create a new Vertex with labels
     *
     * @param labels The labels
     */
    public LPGVertex(LPGGraph lpgGraph, @NonNull String... labels) {
        super(lpgGraph);
        for (String label : labels) {
            this.addLabel(label);
        }
    }

    /**
     * Create a new Vertex with labels list
     *
     * @param labels The labels
     */
    public LPGVertex(LPGGraph lpgGraph, @NonNull List<String> labels) {
        super(lpgGraph);
        for (String label : labels) {
            this.addLabel(label);
        }
    }

    /**
     * Adds new labels to this vertex
     *
     * @param labels The labels
     */
    public void addLabel(@NonNull List<String> labels) {
        for (String label : labels) {
            this.addLabel(label);
        }
    }

    /**
     * Adds a new label to this vertex
     *
     * @param label The label
     */
    public void addLabel(@NonNull String label) {
        this.labels.add(label);
    }

    /**
     * Returns the list of labels this vertex
     *
     * @return The labels of this vertex.
     */
    public Collection<String> labels() {
        return labels;
    }

    public Collection<String> labelsWithSuperVertex() {
        if (CollectionUtil.isEmpty(labels)) {
            return getSuperVertexLabels();
        }
        if (CollectionUtil.isEmpty(getSuperVertexLabels())) {
            return labels;
        }
        List<String> labelAll = new ArrayList<>();
        labelAll.addAll(labels);
        labelAll.addAll(getSuperVertexLabels());
        return labelAll;
    }

    public String stringWithPop() {
        StringBuilder str = new StringBuilder();
        str.append(this);
        for (LPGProperty property : getProperties()) {
            str.append("\nproperties: ").append(property);
        }
        return str.toString();
    }

    @Override
    public String toString() {
        return "(Vertex[" + this.getId() + "] - labels: " + this.labels() + ")";
    }

    @Override
    public String label() {
        return String.join(LABEL_SPLITTER, this.labelsWithSuperVertex());
    }

}
