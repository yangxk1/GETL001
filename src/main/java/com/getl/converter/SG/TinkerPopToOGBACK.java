package com.getl.converter.SG;

import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.onegraph.elements.*;
import com.getl.model.onegraph.statements.OGPropertyStatement;
import com.getl.model.onegraph.statements.OGRelationshipStatement;
import com.getl.model.onegraph.values.OGValue;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.util.*;

/**
 * Performs direct conversion from TinkerPop {@link Graph}s to {@link OGDataset}s
 * without going through the intermediate LPGGraph representation.
 */
public class TinkerPopToOGBACK {

    /**
     * The OneGraph dataset this converter is modifying.
     */
    public final OGDataset dataset;

    /**
     * The mapping configuration
     */
    public final LPGMappingConfiguration configuration;

    /**
     * The delegate for handling conversion events
     */
    public final TinkerPopMapper.TinkerPopConverterDelegate delegate;

    private final SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();

    /**
     * Create a converter with an existing dataset and configuration.
     * @param dataset An existing OneGraph dataset.
     * @param configuration The mapping configuration.
     * @param delegate The converter delegate.
     */
    TinkerPopToOGBACK(@NonNull OGDataset dataset,
                      @NonNull LPGMappingConfiguration configuration,
                      TinkerPopMapper.TinkerPopConverterDelegate delegate) {
        this.dataset = dataset;
        this.configuration = configuration;
        this.delegate = delegate;
    }

    /**
     * Adds the given TinkerPop {@link Graph} to the current {@link #dataset}.
     * @param graph The TinkerPop graph to add.
     */
    public void addTinkerPopGraphToOGDataset(@NonNull Graph graph) {
        // Map TinkerPop vertices to OG SimpleNodes
        Map<Object, OGSimpleNode<?>> vertexIdToNode = new HashMap<>();

        // Process vertices
        Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            OGSimpleNode<?> node = createSimpleNodeForVertex(vertex);
            vertexIdToNode.put(vertex.id(), node);

            // Add label statements
            addStatementsForVertexLabel(node, vertex.label());

            // Add property statements
            addPropertyStatementsForVertex(node, vertex);
        }

        // Process edges
        Iterator<Edge> edges = graph.edges();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            OGSimpleNode<?> outNode = vertexIdToNode.get(edge.outVertex().id());
            OGSimpleNode<?> inNode = vertexIdToNode.get(edge.inVertex().id());

            if (outNode != null && inNode != null) {
                OGRelationshipStatement relStmt = createRelationshipStatementForEdge(edge, outNode, inNode);

                // Add edge property statements
                addPropertyStatementsForEdge(relStmt, edge);
            }
        }
    }

    /**
     * Creates an OGSimpleNode for a TinkerPop vertex
     */
    private OGSimpleNode<?> createSimpleNodeForVertex(Vertex vertex) {
        // Create IRI for the vertex using its ID
        String vertexId = vertex.label() + ":" + vertex.id().toString();
        IRI vertexIRI = valueFactory.createIRI("urn:vertex:", vertexId);
        return dataset.createOrObtainSimpleNode(vertexIRI);
    }

    /**
     * Adds label statement for a vertex
     * Always creates PropertyStatement for labels to avoid creating extra vertices
     */
    private void addStatementsForVertexLabel(OGSimpleNode<?> node, String label) {
        // Always use PropertyStatement for vertex labels (not RelationshipStatement)
        // This prevents labels from being converted to vertices in round-trip conversion
        OGSimpleNodeIRI predSN = dataset.createOrObtainSimpleNode(configuration.defaultNodeLabelPredicate);
        OGValue<String> objValue = new OGValue<>(label);
        dataset.addStatementAndAddToDefaultGraph(new OGPropertyStatement(node, predSN, objValue));
    }

    /**
     * Adds property statements for a vertex
     */
    private void addPropertyStatementsForVertex(OGSimpleNode<?> node, Vertex vertex) {
        Iterator<VertexProperty<Object>> properties = vertex.properties();

        while (properties.hasNext()) {
            VertexProperty<Object> property = properties.next();
            String key = property.key();
            Object value = property.value();

            // Check if value type is supported
            if (!isValueSupported(value)) {
                if (delegate != null) {
                    delegate.unsupportedValueFound(
                        String.format("Property %s with value type %s on vertex %s",
                                    key, value.getClass().getSimpleName(), vertex.id()));
                }
                continue;
            }

            // Check for meta-properties
            Iterator<Property<Object>> metaProps = property.properties();
            if (metaProps.hasNext() && delegate != null) {
                delegate.metaPropertyFound(
                    String.format("Property %s on vertex %s has meta-properties", key, vertex.id()));
            }

            addPropertyStatement(node, key, value);
        }
    }

    /**
     * Creates a relationship statement for a TinkerPop edge
     */
    private OGRelationshipStatement createRelationshipStatementForEdge(Edge edge,
                                                                        OGSimpleNode<?> outNode,
                                                                        OGSimpleNode<?> inNode) {
        OGRelationshipStatement relStmt = new OGRelationshipStatement(outNode, inNode);
        relStmt.edgeIDLPG = edge.id();

        String edgeLabel = edge.label();
        Optional<LPGMappingConfiguration.ELMorPNMEntry<?>> entry = configuration.edgeLabelMapping(edgeLabel);

        if (entry.isPresent()) {
            entry.get().unpack(new LPGMappingConfiguration.MappingEntryDelegate() {
                @Override
                public void handleIRI(IRI i) {
                    relStmt.predicate = dataset.createOrObtainSimpleNode(i);
                }

                @Override
                public void handleString(String s) {
                    relStmt.predicate = new OGPredicateString(s);
                }
            });
        } else {
            // Create predicate from edge label
            IRI edgePredIRI = valueFactory.createIRI("urn:edge:", edgeLabel);
            relStmt.predicate = dataset.createOrObtainSimpleNode(edgePredIRI);
        }

        dataset.addStatementAndAddToDefaultGraph(relStmt);
        return relStmt;
    }

    /**
     * Adds property statements for an edge
     */
    private void addPropertyStatementsForEdge(OGRelationshipStatement relStmt, Edge edge) {
        Iterator<Property<Object>> properties = edge.properties();

        while (properties.hasNext()) {
            Property<Object> property = properties.next();
            String key = property.key();
            Object value = property.value();

            // Check if value type is supported
            if (!isValueSupported(value)) {
                if (delegate != null) {
                    delegate.unsupportedValueFound(
                        String.format("Property %s with value type %s on edge %s",
                                    key, value.getClass().getSimpleName(), edge.id()));
                }
                continue;
            }

            addPropertyStatement(relStmt, key, value);
        }
    }

    /**
     * Adds a property statement for a given subject
     */
    private void addPropertyStatement(OGReifiableElement subject, String propertyName, Object value) {
        OGValue<?> objValue = new OGValue<>(value);

        Optional<LPGMappingConfiguration.ELMorPNMEntry<?>> entry = configuration.propertyNameMapping(propertyName);

        if (entry.isPresent()) {
            entry.get().unpack(new LPGMappingConfiguration.MappingEntryDelegate() {
                @Override
                public void handleIRI(IRI i) {
                    OGPredicate<?> pred = dataset.createOrObtainSimpleNode(i);
                    dataset.addStatementAndAddToDefaultGraph(new OGPropertyStatement(subject, pred, objValue));
                }

                @Override
                public void handleString(String s) {
                    OGPredicate<?> pred = new OGPredicateString(s);
                    dataset.addStatementAndAddToDefaultGraph(new OGPropertyStatement(subject, pred, objValue));
                }
            });
        } else {
            // Create default predicate IRI
            IRI propIRI = valueFactory.createIRI("urn:property:", propertyName);
            OGPredicate<?> pred = dataset.createOrObtainSimpleNode(propIRI);
            dataset.addStatementAndAddToDefaultGraph(new OGPropertyStatement(subject, pred, objValue));
        }
    }

    /**
     * Check if the property value type is supported
     */
    private boolean isValueSupported(Object value) {
        return !(value instanceof Map || value instanceof List || value instanceof Set || value instanceof Collection);
    }
}

