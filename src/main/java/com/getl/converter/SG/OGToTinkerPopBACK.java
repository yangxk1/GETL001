package com.getl.converter.SG;

import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.onegraph.elements.*;
import com.getl.model.onegraph.statements.OGPropertyStatement;
import com.getl.model.onegraph.statements.OGRelationshipStatement;
import com.getl.model.onegraph.values.OGValue;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.util.*;

/**
 * Performs direct conversion from {@link OGDataset}s to TinkerPop {@link Graph}s
 * without going through the intermediate LPGGraph representation.
 */
public class OGToTinkerPopBACK {

    /**
     * The OneGraph dataset to convert from.
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

    /**
     * Create a converter with dataset and configuration.
     * @param dataset The OneGraph dataset to convert from.
     * @param configuration The mapping configuration.
     * @param delegate The converter delegate.
     */
    OGToTinkerPopBACK(@NonNull OGDataset dataset,
                      @NonNull LPGMappingConfiguration configuration,
                      TinkerPopMapper.TinkerPopConverterDelegate delegate) {
        this.dataset = dataset;
        this.configuration = configuration;
        this.delegate = delegate;
    }

    /**
     * Creates a TinkerPop {@link Graph} from the current {@link #dataset}.
     * @return The created TinkerPop graph.
     */
    public Graph createTinkerPopGraphFromOGDataset() {
        TinkerGraph graph = TinkerGraph.open();

        // Map OG nodes to their vertex IDs and labels
        Map<OGSimpleNode<?>, VertexInfo> nodeToVertexInfo = new HashMap<>();
        Map<OGSimpleNode<?>, Vertex> nodeToVertex = new HashMap<>();

        // First pass: collect vertex information (IDs and labels)
        collectVertexInformation(nodeToVertexInfo);

        // Second pass: create vertices
        createVertices(graph, nodeToVertexInfo, nodeToVertex);

        // Third pass: add vertex properties
        addVertexProperties(nodeToVertex);

        // Fourth pass: create edges and their properties
        createEdgesAndProperties(graph, nodeToVertex);

        return graph;
    }

    /**
     * Collects vertex IDs and labels from the dataset
     */
    private void collectVertexInformation(Map<OGSimpleNode<?>, VertexInfo> nodeToVertexInfo) {
        // Collect vertex labels from property statements
        for (OGPropertyStatement propStmt : dataset.getPropertyStatements()) {
            if (propStmt.getSubject() instanceof OGSimpleNode) {
                OGSimpleNode<?> node = (OGSimpleNode<?>) propStmt.getSubject();

                // Check if this is a label/type statement
                String predicateName = getPredicateName(propStmt.getPredicate());
                if (isLabelPredicate(predicateName)) {
                    String label = extractLabelFromValue(propStmt.object);
                    if (label != null) {
                        nodeToVertexInfo.computeIfAbsent(node, k -> new VertexInfo())
                                       .addLabel(label);
                    }
                }

                // Extract vertex ID from node
                String vertexId = extractVertexId(node);
                if (vertexId != null) {
                    nodeToVertexInfo.computeIfAbsent(node, k -> new VertexInfo())
                                   .setId(vertexId);
                }
            }
        }

        // Also collect nodes from relationship statements (only subject and object, NOT predicate)
        for (OGRelationshipStatement relStmt : dataset.getRelationshipStatements()) {
            // Process subject node (out vertex)
            if (relStmt.subject instanceof OGSimpleNode) {
                OGSimpleNode<?> node = (OGSimpleNode<?>) relStmt.subject;
                nodeToVertexInfo.computeIfAbsent(node, k -> new VertexInfo());
                String vertexId = extractVertexId(node);
                if (vertexId != null) {
                    nodeToVertexInfo.get(node).setId(vertexId);
                }
            }

            // Process object node (in vertex)
            if (relStmt.object instanceof OGSimpleNode) {
                OGSimpleNode<?> node = (OGSimpleNode<?>) relStmt.object;
                nodeToVertexInfo.computeIfAbsent(node, k -> new VertexInfo());
                String vertexId = extractVertexId(node);
                if (vertexId != null) {
                    nodeToVertexInfo.get(node).setId(vertexId);
                }
            }

            // Note: relStmt.predicate is the edge label, NOT a vertex!
            // Do NOT collect predicate as a vertex
        }
    }

    /**
     * Creates vertices in the TinkerPop graph
     */
    private void createVertices(TinkerGraph graph,
                                Map<OGSimpleNode<?>, VertexInfo> nodeToVertexInfo,
                                Map<OGSimpleNode<?>, Vertex> nodeToVertex) {
        for (Map.Entry<OGSimpleNode<?>, VertexInfo> entry : nodeToVertexInfo.entrySet()) {
            OGSimpleNode<?> node = entry.getKey();
            VertexInfo info = entry.getValue();

            String label = info.getFirstLabel();
            if (label == null) {
                label = "vertex"; // Default label
            }

            // Warn if multiple labels
            if (info.labels.size() > 1 && delegate != null) {
                delegate.multiLabeledVertexFound(
                    String.format("Vertex %s has multiple labels: %s, only using %s",
                                info.id, info.labels, label));
            }

            // Create vertex with ID and label
            Vertex vertex;
            if (info.id != null) {
                vertex = graph.addVertex(T.id, info.id, T.label, label);
            } else {
                vertex = graph.addVertex(T.label, label);
            }

            nodeToVertex.put(node, vertex);
        }
    }

    /**
     * Adds properties to vertices
     */
    private void addVertexProperties(Map<OGSimpleNode<?>, Vertex> nodeToVertex) {
        for (OGPropertyStatement propStmt : dataset.getPropertyStatements()) {
            if (propStmt.getSubject() instanceof OGSimpleNode) {
                OGSimpleNode<?> node = (OGSimpleNode<?>) propStmt.getSubject();
                Vertex vertex = nodeToVertex.get(node);

                if (vertex != null) {
                    String predicateName = getPredicateName(propStmt.getPredicate());

                    // Skip label predicates as they're already used for vertex labels
                    if (!isLabelPredicate(predicateName)) {
                        Object value = extractValueFromOGValue(propStmt.object);
                        if (value != null) {
                            vertex.property(predicateName, value);
                        }
                    }
                }
            }
        }
    }

    /**
     * Creates edges and their properties
     */
    private void createEdgesAndProperties(TinkerGraph graph, Map<OGSimpleNode<?>, Vertex> nodeToVertex) {
        for (OGRelationshipStatement relStmt : dataset.getRelationshipStatements()) {
            if (relStmt.subject instanceof OGSimpleNode && relStmt.object instanceof OGSimpleNode) {
                Vertex outVertex = nodeToVertex.get(relStmt.subject);
                Vertex inVertex = nodeToVertex.get(relStmt.object);

                if (outVertex != null && inVertex != null) {
                    String edgeLabel = getPredicateName(relStmt.predicate);

                    // Create edge
                    Edge edge;
                    if (relStmt.edgeIDLPG != null) {
                        edge = outVertex.addEdge(edgeLabel, inVertex, T.id, relStmt.edgeIDLPG);
                    } else {
                        edge = outVertex.addEdge(edgeLabel, inVertex);
                    }

                    // Add edge properties
                    addEdgeProperties(edge, relStmt);
                }
            }
        }
    }

    /**
     * Adds properties to an edge
     */
    private void addEdgeProperties(Edge edge, OGRelationshipStatement relStmt) {
        // Find property statements where the edge is the subject
        for (OGPropertyStatement propStmt : dataset.getPropertyStatements()) {
            if (propStmt.getSubject() == relStmt) {
                String predicateName = getPredicateName(propStmt.getPredicate());
                Object value = extractValueFromOGValue(propStmt.object);

                if (value != null) {
                    edge.property(predicateName, value);
                }
            }
        }
    }

    /**
     * Extracts predicate name from OGPredicate
     */
    private String getPredicateName(OGPredicate<?> predicate) {
        if (predicate instanceof OGSimpleNodeIRI) {
            IRI iri = ((OGSimpleNodeIRI) predicate).linkedComponent;
            return iri.getLocalName() != null ? iri.getLocalName() : iri.toString();
        } else if (predicate instanceof OGPredicateString) {
            return ((OGPredicateString) predicate).content;
        } else if (predicate instanceof OGSimpleNodeBNode) {
            return ((OGSimpleNodeBNode) predicate).linkedComponent.getID();
        }
        return "unknown";
    }

    /**
     * Checks if a predicate name represents a label/type predicate
     */
    private boolean isLabelPredicate(String predicateName) {
        return "type".equalsIgnoreCase(predicateName) ||
               predicateName.contains("type") ||
               predicateName.equals(configuration.defaultNodeLabelPredicate.getLocalName());
    }

    /**
     * Extracts label string from OGValue
     */
    private String extractLabelFromValue(OGValue<?> ogValue) {
        if (ogValue == null || ogValue.value == null) {
            return null;
        }

        Object value = ogValue.value;
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Literal) {
            return ((Literal) value).stringValue();
        }
        return value.toString();
    }

    /**
     * Extracts value from OGValue for use as property value
     */
    private Object extractValueFromOGValue(OGValue<?> ogValue) {
        if (ogValue == null || ogValue.value == null) {
            return null;
        }

        Object value = ogValue.value;

        // Convert Literal to appropriate Java type
        if (value instanceof Literal) {
            Literal literal = (Literal) value;
            return com.getl.model.RDF.LiteralConverter.convertToObject(literal);
        }

        return value;
    }

    /**
     * Extracts vertex ID from OGSimpleNode
     */
    private String extractVertexId(OGSimpleNode<?> node) {
        if (node instanceof OGSimpleNodeIRI) {
            IRI iri = ((OGSimpleNodeIRI) node).linkedComponent;
            String iriString = iri.toString();

            // Try to extract ID from IRI (e.g., "urn:vertex:Person:123" -> "Person:123")
            if (iriString.contains(":")) {
                String[] parts = iriString.split(":");
                if (parts.length >= 3) {
                    // Return the part after the namespace
                    return String.join(":", Arrays.copyOfRange(parts, 2, parts.length));
                }
            }
            return iri.getLocalName() != null ? iri.getLocalName() : iriString;
        } else if (node instanceof OGSimpleNodeBNode) {
            return ((OGSimpleNodeBNode) node).linkedComponent.getID();
        }
        return null;
    }

    /**
     * Helper class to store vertex information during conversion
     */
    private static class VertexInfo {
        String id;
        Set<String> labels = new LinkedHashSet<>();

        void setId(String id) {
            if (this.id == null) {
                this.id = id;
            }
        }

        void addLabel(String label) {
            labels.add(label);
        }

        String getFirstLabel() {
            return labels.isEmpty() ? null : labels.iterator().next();
        }
    }
}

