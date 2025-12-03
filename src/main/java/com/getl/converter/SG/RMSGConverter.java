package com.getl.converter.SG;

import com.getl.constant.IRINamespace;
import com.getl.model.RDF.LiteralConverter;
import com.getl.model.RM.Line;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.onegraph.elements.*;
import com.getl.model.onegraph.statements.OGPropertyStatement;
import com.getl.model.onegraph.statements.OGRelationshipStatement;
import com.getl.model.onegraph.values.OGValue;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.util.*;

/**
 * Converter between RMGraph (Relational Model) and OGDataset (OneGraph/SG).
 * Similar to RMMGConverter but for the OneGraph model.
 */
public class RMSGConverter {

    private final RMGraph rmGraph;
    private final OGDataset ogDataset;

    private final Map<String, IRI> iriCache = new HashMap<>();
    private final SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();

    public RMSGConverter(RMGraph rmGraph, OGDataset ogDataset) {
        this.rmGraph = rmGraph;
        this.ogDataset = ogDataset;
    }

    // ---------------- RM -> SG (OGDataset) ----------------

    /**
     * Add the entire RMGraph into OGDataset.
     * Nodes become SimpleNodes with property statements for columns and type statements for labels.
     * Edges become relationship statements between SimpleNodes.
     */
    public void addRMToSG() {
        Map<String, OGSimpleNodeIRI> idMapper = new HashMap<>();
        for (Line line : rmGraph.getLines().values()) {
            handleLineToSG(line, idMapper);
        }
    }

    private OGSimpleNodeIRI handleLineToSG(Line line, Map<String, OGSimpleNodeIRI> idMapper) {
        if (line == null || line.getId() == null) return null;

        OGSimpleNodeIRI node = idMapper.get(line.getId());
        if (node != null) return node;

        String table = line.getTableName();
        Schema schema = rmGraph.getSchemas().get(table);

        // Node row
        if (schema == null || schema.isNode()) {
            IRI nodeIRI = createIRI(IRINamespace.IRI_NAMESPACE, line.getId());
            node = ogDataset.createOrObtainSimpleNode(nodeIRI);
            idMapper.put(line.getId(), node);

            Map<String, Object> values = line.getValues();
            values = values != null ? values : Collections.emptyMap();

            // Properties from columns
            for (Map.Entry<String, Object> e : values.entrySet()) {
                if (e.getKey() == null || e.getValue() == null) continue;
                IRI predIRI = createIRI(IRINamespace.PROPERTIES_NAMESPACE, e.getKey());
                OGSimpleNodeIRI pred = ogDataset.createOrObtainSimpleNode(predIRI);
                Literal lit = LiteralConverter.convertToLiteral(e.getValue());
                OGValue<Literal> objValue = new OGValue<>(lit);

                OGPropertyStatement propStmt = new OGPropertyStatement(node, pred, objValue);
                ogDataset.addStatementAndAddToDefaultGraph(propStmt);
            }

            // Label as type statement
            if (table != null) {
                OGSimpleNodeIRI typePred = ogDataset.createOrObtainSimpleNode(RDF.TYPE);
                Literal labelLit = LiteralConverter.convertToLiteral(table);
                OGValue<Literal> labelValue = new OGValue<>(labelLit);

                OGPropertyStatement typeStmt = new OGPropertyStatement(node, typePred, labelValue);
                ogDataset.addStatementAndAddToDefaultGraph(typeStmt);
            }

            return node;
        }

        // Edge row
        String outCol = schema.getOut();
        String inCol = schema.getIn();
        Object outVal = outCol == null ? null : line.getValues().get(outCol);
        Object inVal = inCol == null ? null : line.getValues().get(inCol);

        if (outVal == null || inVal == null) {
            // Incomplete edge; skip safely
            return null;
        }

        // Resolve/construct endpoint lines or IDs
        String outId = schema.getOutLabel() + ":" + outVal;
        String inId = schema.getInLabel() + ":" + inVal;

        // Endpoint nodes
        Line outLine = rmGraph.getLines().get(outId);
        if (outLine == null) {
            outLine = new Line();
            outLine.setId(outId);
            outLine.setTableName(schema.getOutLabel());
        }
        Line inLine = rmGraph.getLines().get(inId);
        if (inLine == null) {
            inLine = new Line();
            inLine.setId(inId);
            inLine.setTableName(schema.getInLabel());
        }

        OGSimpleNodeIRI outNode = handleLineToSG(outLine, idMapper);
        OGSimpleNodeIRI inNode = handleLineToSG(inLine, idMapper);

        // Edge as relationship statement
        IRI predIRI = createIRI(IRINamespace.EDGE_NAMESPACE, schema.getTableName());
        OGSimpleNodeIRI pred = ogDataset.createOrObtainSimpleNode(predIRI);

        OGRelationshipStatement relStmt = new OGRelationshipStatement(outNode, inNode);
        relStmt.predicate = pred;
        relStmt.edgeIDLPG = line.getId();
        ogDataset.addStatementAndAddToDefaultGraph(relStmt);

        // Edge properties (other columns except in/out)
        for (Map.Entry<String, Object> e : line.getValues().entrySet()) {
            String key = e.getKey();
            Object val = e.getValue();
            if (key == null || val == null) continue;
            if (key.equals(inCol) || key.equals(outCol)) continue;

            IRI propPredIRI = createIRI(IRINamespace.PROPERTIES_NAMESPACE, key);
            OGSimpleNodeIRI propPred = ogDataset.createOrObtainSimpleNode(propPredIRI);
            Literal lit = LiteralConverter.convertToLiteral(val);
            OGValue<Literal> propValue = new OGValue<>(lit);

            OGPropertyStatement edgePropStmt = new OGPropertyStatement(relStmt, propPred, propValue);
            ogDataset.addStatementAndAddToDefaultGraph(edgePropStmt);
        }

        return null; // Edges don't return a node
    }

    // ---------------- SG (OGDataset) -> RM ----------------

    /**
     * Build an RMGraph from the current OGDataset using provided schemas.
     * Creates/updates Lines for nodes, edges, and properties according to schema.
     */
    public void addSGToRM() {
        // Track created node lines by IRI string representation
        Map<String, Line> iriToLine = new HashMap<>();

        // Process property statements for nodes
        for (OGPropertyStatement propStmt : ogDataset.getPropertyStatements()) {
            OGObject subject = propStmt.getSubject();
            OGPredicate<?> predicate = propStmt.getPredicate();
            OGValue<?> object = propStmt.object;

            if (subject instanceof OGSimpleNodeIRI) {
                OGSimpleNodeIRI node = (OGSimpleNodeIRI) subject;
                String nodeId = node.linkedComponent.getLocalName();
                String key = getPredicateName(predicate);
                Object value = convertOGValueToObject(object);

                Line nodeLine = iriToLine.computeIfAbsent(nodeId, id -> {
                    Line l = rmGraph.getLines().get(id);
                    if (l == null) {
                        l = new Line();
                        l.setId(id);
                        l.setTableName(inferNodeLabelFromId(id));
                        rmGraph.getLines().put(id, l);
                    }
                    return l;
                });

                // Skip rdf:type as it's used for table name
                if (!RDF.TYPE.getLocalName().equals(key)) {
                    nodeLine.addValue(key, value);
                } else if (value != null && nodeLine.getTableName() == null) {
                    nodeLine.setTableName(value.toString());
                }
            } else if (subject instanceof OGRelationshipStatement) {
                // Edge property
                OGRelationshipStatement edgeStmt = (OGRelationshipStatement) subject;
                handleEdgePropertyToRM(edgeStmt, predicate, object);
            }
        }

        // Process relationship statements for edges
        for (OGRelationshipStatement relStmt : ogDataset.getRelationshipStatements()) {
            OGReifiableElement subj = relStmt.subject;
            OGReifiableElement obj = relStmt.object;
            OGPredicate<?> pred = relStmt.predicate;

            if (!(subj instanceof OGSimpleNodeIRI) || !(obj instanceof OGSimpleNodeIRI)) {
                continue;
            }

            OGSimpleNodeIRI outNode = (OGSimpleNodeIRI) subj;
            OGSimpleNodeIRI inNode = (OGSimpleNodeIRI) obj;

            String outId = outNode.linkedComponent.getLocalName();
            String inId = inNode.linkedComponent.getLocalName();
            String edgeLabel = getPredicateName(pred);

            Schema schema = rmGraph.getSchemas().get(edgeLabel);
            final String outLabel = (schema != null) ? schema.getOutLabel() : edgeLabel;
            final String inLabel = (schema != null) ? schema.getInLabel() : edgeLabel;
            final String outCol = (schema != null && schema.getOut() != null) ? schema.getOut() : edgeLabel + "_out";
            final String inCol = (schema != null && schema.getIn() != null) ? schema.getIn() : edgeLabel + "_in";

            // Ensure node lines exist
            iriToLine.computeIfAbsent(outId, id -> ensureNodeLine(id, outLabel));
            iriToLine.computeIfAbsent(inId, id -> ensureNodeLine(id, inLabel));

            // Create edge line
            String edgeId = relStmt.edgeIDLPG != null ? String.valueOf(relStmt.edgeIDLPG)
                                                       : outId + "->" + edgeLabel + "->" + inId;
            Line edgeLine = rmGraph.getLines().get(edgeId);
            if (edgeLine == null) {
                edgeLine = new Line();
                edgeLine.setId(edgeId);
                edgeLine.setTableName(edgeLabel);
                rmGraph.getLines().put(edgeId, edgeLine);
            }
            edgeLine.addValue(outCol, stripLabelPrefix(outId, outLabel));
            edgeLine.addValue(inCol, stripLabelPrefix(inId, inLabel));
        }
    }

    // ---------------- Helpers ----------------

    private IRI createIRI(String namespace, String localName) {
        return iriCache.computeIfAbsent(localName, ln -> valueFactory.createIRI(namespace, ln));
    }

    private Line ensureNodeLine(String id, String label) {
        Line l = rmGraph.getLines().get(id);
        if (l == null) {
            l = new Line();
            l.setId(id);
            l.setTableName(label != null ? label : "default_table");
            rmGraph.getLines().put(id, l);
        }
        return l;
    }

    private String inferNodeLabelFromId(String nodeId) {
        // If nodeId has a prefix like label:ID, use the prefix as table name
        int idx = nodeId.indexOf(':');
        if (idx > 0) return nodeId.substring(0, idx);

        // Fallback to a common label from schemas, or default
        return rmGraph.getSchemas().values().stream()
            .filter(Schema::isNode)
            .map(Schema::getTableName)
            .findFirst().orElse("default_table");
    }

    private void handleEdgePropertyToRM(OGRelationshipStatement edgeStmt, OGPredicate<?> predicate, OGValue<?> value) {
        OGReifiableElement subj = edgeStmt.subject;
        OGReifiableElement obj = edgeStmt.object;
        OGPredicate<?> edgePred = edgeStmt.predicate;

        if (!(subj instanceof OGSimpleNodeIRI) || !(obj instanceof OGSimpleNodeIRI)) {
            return;
        }

        OGSimpleNodeIRI outNode = (OGSimpleNodeIRI) subj;
        OGSimpleNodeIRI inNode = (OGSimpleNodeIRI) obj;

        String outId = outNode.linkedComponent.getLocalName();
        String inId = inNode.linkedComponent.getLocalName();
        String edgeLabel = getPredicateName(edgePred);
        String propKey = getPredicateName(predicate);
        Object propValue = convertOGValueToObject(value);

        final String outCol = edgeLabel + "_out";
        final String inCol = edgeLabel + "_in";

        String edgeId = edgeStmt.edgeIDLPG != null ? String.valueOf(edgeStmt.edgeIDLPG)
                                                     : outId + "->" + edgeLabel + "->" + inId;
        Line edgeLine = rmGraph.getLines().get(edgeId);
        if (edgeLine == null) {
            edgeLine = new Line();
            edgeLine.setId(edgeId);
            edgeLine.setTableName(edgeLabel);
            rmGraph.getLines().put(edgeId, edgeLine);
            edgeLine.addValue(outCol, stripLabelPrefix(outId, edgeLabel));
            edgeLine.addValue(inCol, stripLabelPrefix(inId, edgeLabel));
        }
        edgeLine.addValue(propKey, propValue);
    }

    private String stripLabelPrefix(String id, String label) {
        if (id.startsWith(label + ":")) {
            return id.substring(label.length() + 1);
        }
        return id;
    }

    private String getPredicateName(OGPredicate<?> predicate) {
        if (predicate instanceof OGSimpleNodeIRI) {
            return ((OGSimpleNodeIRI) predicate).linkedComponent.getLocalName();
        } else if (predicate instanceof OGPredicateString) {
            return ((OGPredicateString) predicate).content;
        } else if (predicate instanceof OGSimpleNodeBNode) {
            return ((OGSimpleNodeBNode) predicate).linkedComponent.getID();
        }
        return "unknown_predicate";
    }

    private Object convertOGValueToObject(OGValue<?> ogValue) {
        if (ogValue == null || ogValue.value == null) {
            return null;
        }

        Object value = ogValue.value;
        if (value instanceof Literal) {
            return LiteralConverter.convertToObject((Literal) value);
        }
        return value.toString();
    }
}
