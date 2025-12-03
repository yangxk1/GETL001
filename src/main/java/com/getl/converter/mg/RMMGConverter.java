package com.getl.converter.mg;

import com.getl.constant.IRINamespace;
import com.getl.model.MG.MGraph;
import com.getl.model.MG.Statement;
import com.getl.model.RDF.LiteralConverter;
import com.getl.model.RM.Line;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.util.*;

/**
 * Converter between RMGraph (Relational Model) and MGraph (Meta Graph) using
 * conventions from RMConverter (RM<->UG) and PGMapperR4j (PG<->MG).
 */
public class RMMGConverter {

    private final RMGraph rmGraph;
    private final MGraph mGraph;

    private final Map<String, IRI> iriCache = new HashMap<>();

    public RMMGConverter(RMGraph rmGraph, MGraph mGraph) {
        this.rmGraph = rmGraph;
        this.mGraph = mGraph;
    }

    // ---------------- RM -> MG ----------------

    /**
     * Add the entire RMGraph into MGraph.
     * Nodes become IRIs in IRINamespace.IRI_NAMESPACE with rdf:type label literal.
     * Edges become statements using IRINamespace.EDGE_NAMESPACE between out/in subjects.
     * Column values become properties in IRINamespace.PROPERTIES_NAMESPACE.
     */
    public void addRMToMG() {
        Map<String, Resource> idMapper = new HashMap<>();
        for (Line line : rmGraph.getLines().values()) {
            handleLineToMG(line, idMapper);
        }
    }

    private Resource handleLineToMG(Line line, Map<String, Resource> idMapper) {
        if (line == null || line.getId() == null) return null;
        Resource resource = idMapper.get(line.getId());
        if (resource != null) return resource;

        String table = line.getTableName();
        Schema schema = rmGraph.getSchemas().get(table);

        // Node row
        if (schema == null || schema.isNode()) {
            IRI subject = createIRI(IRINamespace.IRI_NAMESPACE, line.getId());
            idMapper.put(line.getId(), subject);

            // Properties from columns
            for (Map.Entry<String, Object> e : line.getValues().entrySet()) {
                if (e.getKey() == null || e.getValue() == null) continue;
                IRI pred = createIRI(IRINamespace.PROPERTIES_NAMESPACE, e.getKey());
                Literal lit = LiteralConverter.convertToLiteral(e.getValue());
                mGraph.add(new Statement(subject, pred, lit));
            }
            // Label
            Literal label = LiteralConverter.convertToLiteral(table != null ? table : "default");
            mGraph.add(new Statement(subject, RDF.TYPE, label));
            return subject;
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

        // Endpoint resources
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
        Resource outRes = handleLineToMG(outLine, idMapper);
        Resource inRes = handleLineToMG(inLine, idMapper);

        // Edge statement
        IRI pred = createIRI(IRINamespace.EDGE_NAMESPACE, schema.getTableName());
        Statement stmt = new Statement(outRes, pred, inRes);
        stmt.setId(line.getId());
        mGraph.add(stmt);

        // Edge properties (other columns except in/out)
        for (Map.Entry<String, Object> e : line.getValues().entrySet()) {
            String key = e.getKey();
            Object val = e.getValue();
            if (key == null || val == null) continue;
            if (key.equals(inCol) || key.equals(outCol)) continue;
            IRI p = createIRI(IRINamespace.PROPERTIES_NAMESPACE, key);
            Literal lit = LiteralConverter.convertToLiteral(val);
            mGraph.add(new Statement(stmt, p, lit));
        }
        return stmt;
    }

    // ---------------- MG -> RM ----------------

    /**
     * Build an RMGraph from the current MGraph using provided schemas.
     * Creates/updates Lines for nodes, edges, and properties according to schema.
     */
    public void addMGToRM() {
        // Track created node lines by IRI localName
        Map<String, Line> idToLine = new HashMap<>();

        for (Statement s : mGraph) {
            Resource subject = s.getSubject();
            IRI predicate = s.getPredicate();
            Value object = s.getObject();

            // Property triple
            if (object instanceof Literal) {
                String key = predicate.getLocalName();
                Object value = LiteralConverter.convertToObject((Literal) object);

                if (subject instanceof IRI) {
                    String nodeId = ((IRI) subject).getLocalName();
                    Line nodeLine = idToLine.computeIfAbsent(nodeId, id -> {
                        Line l = rmGraph.getLines().get(id);
                        if (l == null) {
                            l = new Line();
                            l.setId(id);
                            // Try to infer label from existing schema names in RMGraph or default
                            l.setTableName(inferNodeLabelFromExistingSchemas(id));
                            rmGraph.getLines().put(id, l);
                        }
                        return l;
                    });
                    nodeLine.addValue(key, value);
                } else {
                    // subject is an edge-statement-as-resource (reified for properties)
                    Statement edgeStmt = (Statement) subject;
                    handleEdgeProperty(edgeStmt, key, value);
                }
            }
            // Edge triple
            else {
                if (!(subject instanceof IRI) || !(object instanceof IRI)) {
                    continue; // only support IRI->IRI edges
                }
                String outId = ((IRI) subject).getLocalName();
                String inId = ((IRI) object).getLocalName();
                String edgeLabel = predicate.getLocalName();
                Schema schema = rmGraph.getSchemas().get(edgeLabel);

                final String outLabel = (schema != null) ? schema.getOutLabel() : edgeLabel;
                final String inLabel = (schema != null) ? schema.getInLabel() : edgeLabel;
                final String outCol = (schema != null && schema.getOut() != null) ? schema.getOut() : edgeLabel + "_out";
                final String inCol = (schema != null && schema.getIn() != null) ? schema.getIn() : edgeLabel + "_in";

                // Ensure node lines exist
                idToLine.computeIfAbsent(outId, id -> ensureNodeLine(id, outLabel));
                idToLine.computeIfAbsent(inId, id -> ensureNodeLine(id, inLabel));

                // Create edge line
                String edgeId = s.getId() != null ? String.valueOf(s.getId()) : outId + "->" + edgeLabel + "->" + inId;
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
    }

    // ---------------- helpers ----------------

    private IRI createIRI(String namespace, String localName) {
        return iriCache.computeIfAbsent(localName, ln -> SimpleValueFactory.getInstance().createIRI(namespace, ln));
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

    private String inferNodeLabelFromExistingSchemas(String nodeId) {
        // If nodeId has a prefix like label:ID, use the prefix as table name
        int idx = nodeId.indexOf(':');
        if (idx > 0) return nodeId.substring(0, idx);
        // Fallback to a common label from schemas, or default
        return rmGraph.getSchemas().values().stream()
            .filter(Schema::isNode)
            .map(Schema::getTableName)
            .findFirst().orElse("default_table");
    }

    private void handleEdgeProperty(Statement edgeStmt, String key, Object value) {
        Resource subj = edgeStmt.getSubject();
        org.eclipse.rdf4j.model.IRI pred = edgeStmt.getPredicate();
        Resource obj = (Resource) edgeStmt.getObject();
        if (!(subj instanceof IRI) || !(obj instanceof IRI)) return;
        String outId = ((IRI) subj).getLocalName();
        String inId = ((IRI) obj).getLocalName();
        String edgeLabel = pred.getLocalName();

        final String outLabel = edgeLabel;
        final String inLabel = edgeLabel;
        final String outCol = edgeLabel + "_out";
        final String inCol = edgeLabel + "_in";

        String edgeId = edgeStmt.getId() != null ? String.valueOf(edgeStmt.getId()) : outId + "->" + edgeLabel + "->" + inId;
        Line edgeLine = rmGraph.getLines().get(edgeId);
        if (edgeLine == null) {
            edgeLine = new Line();
            edgeLine.setId(edgeId);
            edgeLine.setTableName(edgeLabel);
            rmGraph.getLines().put(edgeId, edgeLine);
            edgeLine.addValue(outCol, stripLabelPrefix(outId, outLabel));
            edgeLine.addValue(inCol, stripLabelPrefix(inId, inLabel));
        }
        edgeLine.addValue(key, value);
    }

    private String stripLabelPrefix(String id, String label) {
        if (id.startsWith(label + ":")) {
            return id.substring(label.length() + 1);
        }
        return id;
    }
}
