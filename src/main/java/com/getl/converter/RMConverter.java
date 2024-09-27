package com.getl.converter;

import com.getl.model.ug.*;
import com.getl.model.LPG.*;
import com.getl.model.RDF.LiteralConverter;
import com.getl.model.RM.Line;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import lombok.NonNull;

import java.util.*;

import static com.getl.constant.IRINamespace.EDGE_NAMESPACE;
import static com.getl.constant.IRINamespace.IRI_NAMESPACE;

public class RMConverter {

    private Set<IRI> typedIRI = new HashSet<>();

    /**
     * The UG dataset this mapper is currently modifying.
     */
    public final UnifiedGraph unifiedGraph;
    public LPGGraph lpgGraph;
    public final RMGraph rmGraph;

    /**
     * Creates the mapper using an existing OneGraph dataset
     *
     * @param unifiedGraph An existing OneGraph dataset
     */
    public RMConverter(@NonNull UnifiedGraph unifiedGraph, RMGraph rmGraph) {
        this.rmGraph = rmGraph;
        this.unifiedGraph = unifiedGraph;
    }

    public RMConverter(RMGraph rmGraph) {
        this.rmGraph = rmGraph;
        this.unifiedGraph = new UnifiedGraph();
    }

    private Map<String, Pair> idToPair = new HashMap<>();

    public void handleLine(Line line) {
        String label = line.getTableName();
        Schema schema = rmGraph.getSchemas().get(label);
        Pair pair = trans2Pair(idToPair, line, schema);
        for (Map.Entry<String, Object> pop : line.getValues().entrySet()) {
            if (!schema.isNode() && (schema.getIn().equals(pop.getKey()) || schema.getOut().equals(pop.getKey()))) {
                continue;
            }
            if (pop.getKey() == null || pop.getValue() == null) {
                continue;
            }
            ConstantPair constantPair = LiteralConverter.convertToUGMLiteral(pop.getValue());
            IRI popName = unifiedGraph.getOrRegisterPopIRI(pop.getKey());
            unifiedGraph.add(popName, pair, constantPair);
        }
    }


    /**
     * Adds Relation mode data to the current UG dataset
     */
    public void addRMModelToUGM() {
        for (Line line : rmGraph.getLines().values()) {
            handleLine(line);
        }
    }

    private Pair trans2Pair(Map<String, Pair> idToPair, Line line, Schema schema) {
        Pair pair = idToPair.get(line.getId());
        if (pair != null) {
            return pair;
        }
        if (schema == null || schema.isNode()) {
            //node
            pair = unifiedGraph.getOrRegisterBasePair(IRI_NAMESPACE, line.getId());
            if (line.getTableName() != null) {
                unifiedGraph.addLabel((BasePair) pair, unifiedGraph.getOrRegisterLabel(line.getTableName()));
            }
        } else {
            //edge
            IRI edgeLabel = unifiedGraph.getOrRegisterBaseIRI(EDGE_NAMESPACE, schema.getTableName());
            String outId = Optional.of(line).map(Line::getValues).map(m -> m.get(schema.getOut())).map(id -> schema.getOutLabel() + ":" + id).orElse(null);
            if (outId == null) {
                return null;
            }
            Line out = Optional.of(outId).map(rmGraph.getLines()::get).orElse(null);
            if (out == null) {
                out = new Line();
                out.setId(outId);
                out.setTableName(schema.getOutLabel());
            }
            String inId = Optional.of(line).map(Line::getValues).map(m -> m.get(schema.getIn())).map(id -> schema.getInLabel() + ":" + id).orElse(null);
            if (inId == null) {
                return null;
            }
            Line in = Optional.of(inId).map(rmGraph.getLines()::get).orElse(null);
            if (in == null) {
                in = new Line();
                in.setId(inId);
                in.setTableName(schema.getInLabel());
            }
            Schema outSM = Optional.of(out).map(Line::getTableName).map(rmGraph.getSchemas()::get).orElse(null);
            Schema inSM = Optional.of(in).map(Line::getTableName).map(rmGraph.getSchemas()::get).orElse(null);
            Pair outIRI = trans2Pair(idToPair, out, outSM);
            Pair inIRI = trans2Pair(idToPair, in, inSM);
            pair = unifiedGraph.add(edgeLabel, line.getId(), outIRI, inIRI);
            pair = ((NestedPair) pair).from();
        }
        idToPair.put(line.getId(), pair);
        return pair;
    }

    public void addUGMToRMModel() {
        for (BasePair basePair : unifiedGraph.getIRI2BasePair().values()) {
            handleBasePair(basePair);
        }
        for (NestedPair nestedPair : unifiedGraph.getPairs()) {
            handleLine(nestedPair);
        }
    }

    private Line handleBasePair(BasePair basePair) {
        Line line = null;
        if (basePair.getContent() == null) {
            line = rmGraph.getLines().get(basePair.to().getLocalName());
            if (line == null) {
                line = new Line();
                line.setId(basePair.to().getLocalName());
                Iterator<IRI> iterator = basePair.from().iterator();
                String tableName = iterator.hasNext() ? iterator.next().getLocalName() : "default_table";
                line.setTableName(tableName);
                rmGraph.getLines().put(line.getId(), line);
            }
        }
        //subject is nestedPair
        else {
            line = handleLine(basePair.getContent());
        }
        return line;
    }

    private Line handleLine(NestedPair nestedPair) {
        Line line = rmGraph.getLines().get(nestedPair.getID());
        if (line != null) {
            return line;
        }
        //subject is node
        String label = nestedPair.from().from().iterator().next().getLocalName();
        Pair inPair = nestedPair.to().to();
        BasePair outPair = nestedPair.to().from();
        Line out = handleBasePair(outPair);
        //edge
        if (rmGraph.getSchemas().containsKey(label)) {
            Schema schema = rmGraph.getSchemas().get(label);
            if (schema.isNode()) {
                throw new RuntimeException("ERROR: nestedPari is node" + nestedPair);
            }
            line = rmGraph.getLines().get(nestedPair.getID());
            if (line == null) {
                line = new Line();
                line.setId(nestedPair.getID());
                line.setTableName(label);
                rmGraph.getLines().put(line.getId(), line);
            }
            String outID = out.getId();
            line.addValue(schema.getOut(), outID);
            Object in = null;
            if (inPair instanceof BasePair) {
                in = handleBasePair((BasePair) inPair).getId();
            } else {
                //ConstantPair
                in = inPair.to();
            }
            line.addValue(schema.getIn(), in);
            return line;
        }
        //property
        Object value = null;
        if (inPair instanceof ConstantPair) {
            ConstantPair in = (ConstantPair) inPair;
            value = in.to();
        } else if (inPair instanceof BasePair) {
            value = handleBasePair((BasePair) inPair).getId();
        }
        out.addValue(label, value);
        return out;
    }
}
