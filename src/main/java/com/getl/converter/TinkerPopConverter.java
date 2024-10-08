package com.getl.converter;

import com.getl.model.ug.*;
import com.getl.model.RDF.LiteralConverter;
import com.getl.model.ug.IRI;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

import static com.getl.constant.IRINamespace.*;

/**
 * Performs conversions between TinkerPop {@link Graph}s and {@link UnifiedGraph}s in both directions.
 */
public class TinkerPopConverter {
    public UnifiedGraph unifiedGraph;
    public Map<String, PropertiesGraphConfig> lpgConfigs;
    public Graph lpgGraph;
    private PropertiesGraphConfig defaultConfig;

    public TinkerPopConverter(UnifiedGraph unifiedGraph, Graph lpgGraph) {
        this(unifiedGraph, lpgGraph, new HashMap<>());
        defaultConfig = new PropertiesGraphConfig();
        defaultConfig.addEdgeNamespaceList(EDGE_NAMESPACE);
    }

    public TinkerPopConverter(UnifiedGraph unifiedGraph, Graph lpgGraph, Map<String, PropertiesGraphConfig> lpgConfigs) {
        this.unifiedGraph = unifiedGraph;
        this.lpgGraph = lpgGraph;
        this.lpgConfigs = lpgConfigs;
        this.defaultConfig = new PropertiesGraphConfig();
    }

    public UnifiedGraph createUGMFromTinkerPopGraph() {
        assert this.lpgGraph != null;
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        this.unifiedGraph = unifiedGraph;
        addUGMFromTinkerPopGraph();
        return unifiedGraph;
    }

    private UnifiedGraph addUGMFromTinkerPopGraph() {
        assert this.lpgGraph != null;
        //element id -> IRI
        Map<Object, Pair> vertexes = new HashMap<>();
        // Adds statements for the vertices
        lpgGraph.vertices().forEachRemaining(vertex -> transElementToUGMIRI(this.unifiedGraph, this.lpgGraph, vertexes, vertex));
        lpgGraph.edges().forEachRemaining(edge -> transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, edge));
        return unifiedGraph;
    }

    private Pair transElementToUGMIRI(UnifiedGraph unifiedGraph, Graph lpgGraph, Map<Object, Pair> vertexes, Element element) {
        if (vertexes.containsKey(element.id())) {
            return vertexes.get(element.id());
        }
        //处理element的公共属性
        Pair result = null;
        if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;
            result = transVerticesToUGMIRI(unifiedGraph, lpgGraph, vertexes, vertex);
            vertexes.put(element.id(), result);
        } else if (element instanceof Edge) {
            result = transEdgeToUGMIRI(unifiedGraph, lpgGraph, vertexes, (Edge) element);
            vertexes.put(element.id(), result);
        }
        Iterator<? extends Property<Object>> properties = element.properties();
        while (properties.hasNext()) {
            Property<Object> pop = properties.next();
            ConstantPair constantPair = LiteralConverter.convertToUGMLiteral(pop.value());
            IRI popIRI = unifiedGraph.getOrRegisterPopIRI(pop.key());
            unifiedGraph.add(popIRI, result, constantPair);
        }
        return result;
    }

    private Pair transVerticesToUGMIRI(UnifiedGraph unifiedGraph, Graph lpgGraph, Map<Object, Pair> vertexes, Vertex vertex) {
        Pair vertexIRI = vertexes.get(vertex.id());
        if (vertexIRI == null) {
            String label = vertex.label();
            PropertiesGraphConfig propertiesGraphConfig = lpgConfigs.computeIfAbsent(label, i -> defaultConfig);
            IRI labelIRI = propertiesGraphConfig.mapLabel(unifiedGraph, label);
            IRI iDtoIRI = propertiesGraphConfig.getIdTransform().IDtoIRI(vertex.id(), unifiedGraph);
            vertexIRI = unifiedGraph.addLabel(iDtoIRI, labelIRI);
            vertexes.put(vertex.id(), vertexIRI);
        }
        return vertexIRI;
    }

    private Pair transEdgeToUGMIRI(UnifiedGraph unifiedGraph, Graph lpgGraph, Map<Object, Pair> vertexes, Edge lpgEdge) throws ConverterException {
        Pair pair = vertexes.get(lpgEdge.id());
        if (pair == null) {
            // Get the LPG vertices that correspond to the lpg vertices.
            Pair outV = vertexes.get(lpgEdge.outVertex().id());
            Pair inV = vertexes.get(lpgEdge.inVertex().id());
            if (outV == null) {
                outV = transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, lpgEdge.outVertex());
            }
            if (inV == null) {
                inV = transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, lpgEdge.inVertex());
            }
            String label = lpgEdge.label();
            PropertiesGraphConfig propertiesGraphConfig = lpgConfigs.computeIfAbsent(label, i -> defaultConfig);
            String edge1 = propertiesGraphConfig.getEdge(label);
            IRI edge = unifiedGraph.getOrRegisterLabel(EDGE_NAMESPACE, edge1);
            pair = unifiedGraph.add(edge, lpgEdge.id(), outV, inV).from();
            vertexes.put(lpgEdge.id(), pair);
        }
        return pair;
    }

    Map<Object, Pair> vertexes = new HashMap<>();

    public void handleElement(Element element) {
        transElementToUGMIRI(unifiedGraph, lpgGraph, vertexes, element);
    }
}
