package com.getl.converter.SG;

import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.onegraph.elements.OGReifiableElement;
import com.getl.model.onegraph.elements.OGSimpleNodeIRI;
import com.getl.model.onegraph.statements.OGPropertyStatement;
import com.getl.model.onegraph.statements.OGRelationshipStatement;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

/**
 * Performs direct conversion from {@link OGDataset}s to TinkerPop {@link Graph}s
 * without going through the intermediate LPGGraph representation.
 */
public class OGToTinkerPop {

    /**
     * The OneGraph dataset this mapper is currently modifying.
     */
    public final OGDataset dataset;

    /**
     * The mapping configuration
     */
    public final LPGMappingConfiguration configuration;

    /**
     * Create a mapper with an existing dataset and existing configuration.
     *
     * @param dataset       An existing OneGraph dataset.
     * @param configuration The mapping configuration.
     */
    OGToTinkerPop(@NonNull OGDataset dataset,
                  @NonNull LPGMappingConfiguration configuration) {
        this.dataset = dataset;
        this.configuration = configuration;
    }

    /**
     * Creates a {@link LPGGraph} from the current {@link #dataset}.
     *
     * @return The labeled property graph.
     */
    public LPGGraph createLPGFromOGDataset() {
        LPGGraph g = new LPGGraph();
        Map<OGReifiableElement, LPGVertex> elemToVertex = new HashMap<>();
        Map<OGReifiableElement, LPGEdge> elemToEdge = new HashMap<>();

        // First add vertices for all simple nodes (that have a vertex ID as linked component)
        for (OGSimpleNodeIRI sn : this.dataset.getVertexSimpleNodes()) {
            LPGVertex v = new LPGVertex(g);
            elemToVertex.put(sn, v);
        }
        // Then handle all relationship statements.
        for (OGRelationshipStatement statement : this.dataset.getRelationshipStatements()) {
            if (!this.dataset.isStatementAsserted(statement)) {
                continue;
            }
            Optional<LPGEdge> e = this.handleRelationshipStatement(statement, g, elemToVertex);
            e.ifPresent(lpgEdge -> elemToEdge.put(statement, lpgEdge));
        }
        // Then handle all property statements.
        for (OGPropertyStatement statement : this.dataset.getPropertyStatements()) {
            if (!this.dataset.isStatementAsserted(statement)) {
                continue;
            }
            this.handlePropertyStatement(statement, g, elemToVertex, elemToEdge);
        }
        return g;
    }

    // Adds vertex and edge properties to the given LPG from the given property statement.
    // Only property statements that are of the following form can be translated to LPG:
    // 1. SN - pred - value
    // 2. RelStat - pred - value, and RelStat must be a key in the mapping relToEdge.
    private void handlePropertyStatement(OGPropertyStatement prop, LPGGraph g, Map<OGReifiableElement, LPGVertex> elemToVertex,
                                         Map<OGReifiableElement, LPGEdge> elemToEdge) {
        if (prop.canBecomeNodeProperty()) {
            // This property statement is about a node property or node label.
            LPGVertex v = createOrObtainVertex(g, prop.subject, elemToVertex);
            g.addVertex(v);
            Optional<String> optLabel = prop.getNodeLabelMappingEntry(this.configuration);
            if (optLabel.isPresent()) {
                v.addLabel(optLabel.get());
            } else {
                v.addPropertyValue(prop.obtainPropName(this.configuration), prop.object.lpgValue());
            }
        } else {
            // This property statement is about an edge property.
            if (elemToEdge.containsKey(prop.subject)) {
                LPGEdge e = elemToEdge.get(prop.subject);
                e.addPropertyValue(prop.obtainPropName(this.configuration), prop.object.lpgValue());
            }
        }
    }

    // Adds new elements to the given LPG inferred from the given relationship statement.
    // Only relationship statements that are of the following form can be translated to LPG:
    // 1. SN - pred - SN
    private Optional<LPGEdge> handleRelationshipStatement(OGRelationshipStatement rel, LPGGraph g,
                                                          Map<OGReifiableElement, LPGVertex> elemToVertex) {
        // Distinguish between relationship statements that say something about a vertex label,
        // and relationship statements that are an edge between 2 vertices.
        if (rel.getNodeLabelMappingEntry(this.configuration).isPresent()) {
            String label = rel.getNodeLabelMappingEntry(this.configuration).get();
            LPGVertex v = createOrObtainVertex(g, rel.subject, elemToVertex);
            g.addVertex(v);
            v.addLabel(label);
            // This relationship statement did not spawn an edge in LPG so return empty.
            return Optional.empty();
        } else if (rel.canBecomeEdge()) {
            LPGVertex outV = createOrObtainVertex(g, rel.subject, elemToVertex);
            LPGVertex inV = createOrObtainVertex(g, rel.object, elemToVertex);

            LPGEdge edge = new LPGEdge(g, outV, inV, rel.obtainEdgeLabel(this.configuration));
            rel.elementId().ifPresent(edge::setId);
            g.addVertex(outV);
            g.addVertex(inV);
            g.addEdge(edge);
            return Optional.of(edge);
        } else {
            // The relationship statement can not be translated to LPG
            return Optional.empty();
        }
    }

    // Either obtains an existing vertex from the elemToVertex map, or creates and adds
    // a new one. Sets the vertex id.
    public LPGVertex createOrObtainVertex(LPGGraph g, OGReifiableElement elem, Map<OGReifiableElement, LPGVertex> elemToVertex) {
        LPGVertex outV = elemToVertex.computeIfAbsent(elem, k -> new LPGVertex(g));
        elem.elementId().ifPresent(outV::setId);
        return outV;
    }
}

