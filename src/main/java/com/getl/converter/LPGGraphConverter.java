package com.getl.converter;


import cn.hutool.core.collection.CollectionUtil;
import com.getl.model.ug.NestedPair;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.*;
import com.getl.model.RDF.LiteralConverter;
import com.getl.model.ug.*;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.getl.constant.IRINamespace.*;

@Data
public class LPGGraphConverter {
    public UnifiedGraph UnifiedGraph;
    public Map<String, PropertiesGraphConfig> lpgConfigs;
    public Map<String, LPGElement> elementCache;
    private PropertiesGraphConfig defaultConfig;
    public final Map<String, LPGElement> elementMap = new HashMap<>();
    public LPGGraph lpgGraph;

    public LPGGraphConverter(UnifiedGraph unifiedGraph, LPGGraph lpgGraph, Map<String, PropertiesGraphConfig> lpgConfigs) {
        this.UnifiedGraph = unifiedGraph;
        this.lpgGraph = lpgGraph;
        this.lpgConfigs = lpgConfigs;
        this.defaultConfig = new PropertiesGraphConfig();
        defaultConfig = new PropertiesGraphConfig();
        defaultConfig.addEdgeNamespaceList(EDGE_NAMESPACE);
    }

    public UnifiedGraph createUGMFromLPGGraph() {
        assert this.lpgGraph != null;
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        this.UnifiedGraph = unifiedGraph;
        addUGMFromLPGGraph();
        return unifiedGraph;
    }

    public void addUGMFromLPGGraph() {
        assert this.lpgGraph != null;
        UnifiedGraph unifiedGraph = this.UnifiedGraph;
        Map<LPGElement, Pair> vertexes = new HashMap<>();
        // Adds statements for the vertices
        lpgGraph.getVertices().stream().filter(i -> !(i instanceof LPGSuperVertex))
                .forEach(vertex -> transElementToUGMIRI(unifiedGraph, this.lpgGraph, vertexes, vertex));
        // Adds statements for the edges
        lpgGraph.getEdges().forEach(edge -> transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, edge));
    }

    private Pair transVerticesToUGMIRI(UnifiedGraph unifiedGraph, LPGGraph lpgGraph, Map<LPGElement, Pair> vertexes, LPGVertex vertex) {
        Pair vertexIRI = vertexes.get(vertex);
        if (vertexIRI != null) {
            return vertexIRI;
        }
        for (String label : vertex.labels()) {
            PropertiesGraphConfig propertiesGraphConfig = lpgConfigs.computeIfAbsent(label, i -> defaultConfig);
            IRI labelIRI = propertiesGraphConfig.mapLabel(unifiedGraph, label);
            IRI iDtoIRI = propertiesGraphConfig.getIdTransform().IDtoIRI(vertex.getId(), unifiedGraph);
            vertexIRI = unifiedGraph.addLabel(iDtoIRI, labelIRI);
        }
        return vertexIRI;
    }

    /**
     * mapper any LPG element to UG
     */
    private Pair transElementToUGMIRI(UnifiedGraph unifiedGraph, LPGGraph lpgGraph, Map<LPGElement, Pair> vertexes, LPGElement element) {
        if (vertexes.containsKey(element)) {
            return vertexes.get(element);
        }
        Pair result = null;
        if (element instanceof LPGVertex) {
            result = transVerticesToUGMIRI(unifiedGraph, lpgGraph, vertexes, (LPGVertex) element);
        } else if (element instanceof LPGEdge) {
            result = ((NestedPair) transEdgeToUGMIRI(unifiedGraph, lpgGraph, vertexes, (LPGEdge) element)).from();
        } else if (element instanceof LPGProperty) {
            result = transPropertiesToUGMIRI(unifiedGraph, lpgGraph, vertexes, (LPGProperty) element);
        }
        vertexes.put(element, result);
        for (LPGProperty property : element.getProperties()) {
            transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, property);
        }
        return result;
    }

    private Pair transPropertiesToUGMIRI(UnifiedGraph unifiedGraph, LPGGraph lpgGraph, Map<LPGElement, Pair> vertexes, LPGProperty element) {
        Pair pair = vertexes.get(element);
        if (pair != null) {
            return pair;
        }
        LPGElement key = element.element;
        String label = "";
        label = key.label();
        PropertiesGraphConfig propertiesGraphConfig = lpgConfigs.computeIfAbsent(label, i -> defaultConfig);
        Pair elementIRI = vertexes.computeIfAbsent(key, i -> transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, i));
        Object value = element.value;
        Pair valueIRI;
        if (value instanceof LPGElement) {
            valueIRI = vertexes.computeIfAbsent((LPGElement) value, i -> transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, i));
        } else {
            valueIRI = LiteralConverter.convertToUGMLiteral(value);
        }
        IRI pop = propertiesGraphConfig.getPop(unifiedGraph, element.name);
        return unifiedGraph.add(pop, (BasePair) elementIRI, valueIRI);
    }

    private Pair transEdgeToUGMIRI(UnifiedGraph unifiedGraph, LPGGraph lpgGraph, Map<LPGElement, Pair> vertexes, LPGEdge lpgEdge) throws ConverterException {
        Pair pair = vertexes.get(lpgEdge);
        if (pair != null) {
            return pair;
        }
        // Get the LPG vertices that correspond to the lpg vertices.
        Pair outV = vertexes.get(lpgEdge.outVertex);
        Pair inV = vertexes.get(lpgEdge.inVertex);
        if (outV == null) {
            outV = transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, lpgEdge.outVertex);
        }
        if (inV == null) {
            inV = transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, lpgEdge.inVertex);
        }
        String label = "";
        if (lpgEdge.outVertex instanceof LPGVertex) {
            label = ((LPGVertex) lpgEdge.outVertex).labels().stream().findFirst().orElse("vertex");
        } else if (lpgEdge.outVertex instanceof LPGEdge) {
            label = ((LPGEdge) lpgEdge.outVertex).label;
        }
        PropertiesGraphConfig propertiesGraphConfig = lpgConfigs.computeIfAbsent(label, i -> defaultConfig);
        String edgeLabel = propertiesGraphConfig.getEdge(lpgEdge.label);
        IRI edge = unifiedGraph.getOrRegisterLabel(EDGE_NAMESPACE, edgeLabel);
        return unifiedGraph.add(edge, lpgEdge.getId(), outV, inV);
    }

    /**
     * mapper UG to LPG
     */
    public LPGGraph createLPGGraphByUGM() {
        this.lpgGraph = new LPGGraph();
        elementCache = new HashMap<>();
        for (NestedPair nestedPair : this.getUnifiedGraph().getPairs()) {
            handlePair(nestedPair);
        }
        return lpgGraph;
    }

    private LPGElement handlePair(NestedPair nestedPair) {
        LPGElement element = elementCache.get(nestedPair.getID());
        if (element != null) {
            return element;
        }
        LPGElement out = null;
        Object in = null;
        //relation out -> in
        //outV is node ro edge
        out = handleElement(nestedPair.to().from());
        //inV is constant
        if (nestedPair.to().to() instanceof ConstantPair) {
            ConstantPair inL = (ConstantPair) nestedPair.to().to();
            in = inL.to();
        }
        //inV is node or edge
        else if (nestedPair.to().to() instanceof BasePair) {
            in = handleElement((BasePair) nestedPair.to().to());
        }
        BasePair predicate = nestedPair.from();
        //edges and properties only have one label
        IRI predicateIRI = predicate.from().iterator().next();
        List<PropertiesGraphConfig> list;
        if (out instanceof LPGEdge || out instanceof LPGProperty) {
            list = Collections.singletonList(Optional.of(out.label()).map(lpgConfigs::get).orElse(defaultConfig));
        } else {
            LPGVertex lpgVertex = (LPGVertex) out;
            list = (List) Optional.of(lpgVertex.labels())
                    .orElse(Collections.EMPTY_LIST)
                    .stream()
                    .filter(Objects::nonNull)
                    .map(lpgConfigs::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        if (CollectionUtil.isEmpty(list)) {
            list = Collections.singletonList(defaultConfig);
        }
        for (PropertiesGraphConfig config : list) {
            LPGElement lpgElement = handleRelation(nestedPair, out, predicateIRI, in, config);
            if (lpgElement != null) {
                elementCache.put(nestedPair.getID(), lpgElement);
                return lpgElement;
            }
        }
        return null;
    }

    public LPGElement handleRelation(NestedPair nestedPair, LPGElement out, IRI predicateIRI, Object in, PropertiesGraphConfig propertiesGraphConfig) {
        String label = propertiesGraphConfig.epLabel(predicateIRI);
        if (StringUtils.isBlank(label)) {
            label = predicateIRI.getLocalName();
        }
        boolean isEdge = propertiesGraphConfig.isEdge(predicateIRI);
        if (isEdge || (in instanceof LPGElement)) {
            //edge
            LPGEdge lpgEdge = new LPGEdge(lpgGraph, out, (LPGElement) in, label);
            lpgEdge.setId(nestedPair.getID());
            return lpgEdge;
        } else {
            //property
            LPGProperty lpgProperty = new LPGProperty(lpgGraph, label, out, in);
            lpgProperty.setId(nestedPair.getID());
            out.addProperty(lpgProperty);
            return lpgProperty;
        }
    }

    public LPGElement handleElement(BasePair basePair) {
        if (basePair.getContent() != null) {
            //edge or property
            return handlePair(basePair.getContent());
        }
        LPGElement element = elementCache.get(basePair.to().getLocalName());
        if (element == null) {
            LPGVertex vertex = new LPGVertex(lpgGraph);
            element = vertex;
            for (IRI label : basePair.from()) {
                String labelStr = label.getLocalName();
                PropertiesGraphConfig propertiesGraphConfig = Optional.of(labelStr).map(lpgConfigs::get).orElse(defaultConfig);
                vertex.addLabel(propertiesGraphConfig.mapLabel(label));
            }
            element.setId(basePair.to().getLocalName());
            lpgGraph.addVertex(vertex);
            elementCache.put(basePair.to().getLocalName(), element);
        }
        return element;
    }
}
