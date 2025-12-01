package com.getl.converter.mg;

import com.getl.constant.IRINamespace;
import com.getl.model.MG.MGraph;
import com.getl.model.MG.Statement;
import com.getl.model.RDF.LiteralConverter;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set;

public class PGMapperR4j implements PGMapperI {
    @Getter
    @Setter
    private MGraph mGraph;
    private Map<String, IRI> IRICache = new HashMap<>();

    //    private
    public PGMapperR4j(MGraph mGraph) {
        this.mGraph = mGraph;
    }

    public void addPGToMG(@NonNull Graph graph) {
        Iterator<Vertex> vertices = graph.vertices();
        Map<Object, Resource> idMapper = new HashMap<>();
        while (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            addVertexToMG(vertex, idMapper);
        }
    }

    private void addEdgeToMG(Edge edge, Map<Object, Resource> idMapper) {
        if (idMapper.get(edge.id()) != null) {
            return;
        }
        Vertex outVertex = edge.outVertex();
        Vertex inVertex = edge.inVertex();
        Resource outIRI = idMapper.get(outVertex.id());
        if (outIRI == null) {
            outIRI = addVertexToMG(outVertex, idMapper);
        }
        Resource inIRI = idMapper.get(inVertex.id());
        if (inIRI == null) {
            inIRI = addVertexToMG(inVertex, idMapper);
        }
        IRI predicate = createIRI(IRINamespace.EDGE_NAMESPACE, edge.label());
        Statement statement = new Statement(outIRI, predicate, inIRI);
        statement.setId(edge.id());
        mGraph.add(statement);
        idMapper.put(edge.id(), statement);
        Iterator<Property<Object>> properties = edge.properties();
        while (properties.hasNext()) {
            Property<Object> pop = properties.next();
            addPopToMG(pop, statement);
        }
    }

    private Resource addVertexToMG(Vertex vertex, Map<Object, Resource> idMapper) {
        Resource resource = idMapper.get(vertex.id());
        if (resource != null) {
            return resource;
        }
        IRI vertexIRI = createIRI(IRINamespace.IRI_NAMESPACE, vertex.id().toString());
        idMapper.put(vertex.id(), vertexIRI);
        Iterator<VertexProperty<Object>> properties = vertex.properties();
        while (properties.hasNext()) {
            VertexProperty<Object> pop = properties.next();
            addPopToMG(pop, vertexIRI);
        }
        vertex.edges(Direction.OUT).forEachRemaining(edge -> addEdgeToMG(edge, idMapper));
        Literal label = LiteralConverter.convertToLiteral(vertex.label());
        Statement statement = new Statement(vertexIRI, RDF.TYPE, label);
        mGraph.add(statement);
        return vertexIRI;
    }

    private void addPopToMG(Property property, Resource source) {
        String key = property.key();
        Object value = property.value();
        IRI predicate = createIRI(IRINamespace.PROPERTIES_NAMESPACE, key);
        Literal literal = LiteralConverter.convertToLiteral(value);
        Statement statement = new Statement(source, predicate, literal);
        mGraph.add(statement);
    }

    public IRI createIRI(String namespace, String localName) {
        IRI iri = IRICache.get(localName);
        if (iri == null) {
            ValueFactory factory = SimpleValueFactory.getInstance();
            iri = factory.createIRI(namespace, localName);
            IRICache.put(localName, iri);
        }
        return iri;
    }

    public Graph createGraphFromMG() {
        TinkerGraph graph = TinkerGraph.open();
        Map<String, Element> idMapper = new HashMap<>();
        for (Statement statement : mGraph) {
            createElementFromStatement(graph, statement, idMapper);
        }
        return graph;
    }

    private Element createElementFromStatement(TinkerGraph graph, Statement statement, Map<String, Element> idMapper) {
        Element element = idMapper.get(statement.stringValue());
        if (element != null) {
            return element;
        }
        Resource subject = statement.getSubject();
        IRI predicate = statement.getPredicate();
        Value object = statement.getObject();
        element = getElement(graph, idMapper, subject);
        if (object instanceof Literal) {
            String key = predicate.getLocalName();
            Object value = LiteralConverter.convertToObject((Literal) (object));
            if (element instanceof Vertex) {
                ((Vertex) element).property(set, key, value);
            } else {
                element.property(key, value);
            }
            return null;
        } else {
            Element objectEle = getElement(graph, idMapper, (Resource) object);
            Vertex from, to;
            if (objectEle instanceof Edge) {
                Iterator<Vertex> vertices = graph.vertices("e" + objectEle.id());
                if (vertices.hasNext()) {
                    to = vertices.next();
                } else {
                    to = graph.addVertex(T.label, objectEle.label(), T.id, "e" + objectEle.id());
                }
            } else {
                to = (Vertex) objectEle;
            }
            if (element instanceof Edge) {
                Iterator<Vertex> vertices = graph.vertices("e" + element.id());
                if (vertices.hasNext()) {
                    from = vertices.next();
                } else {
                    from = graph.addVertex(T.label, element.label(), T.id, "e" + element.id());
                }
            } else {
                from = (Vertex) element;
            }
            Edge edge;
            if (statement.getId() != null) {
                edge = from.addEdge(predicate.getLocalName(), to, T.id, statement.getId());
            } else {
                edge = from.addEdge(predicate.getLocalName(), to);
            }
            idMapper.put(statement.stringValue(), edge);
            return edge;
        }
    }

    private Element getElement(TinkerGraph graph, Map<String, Element> idMapper, Resource resource) {
        Element element = idMapper.get(resource.stringValue());
        if (element != null) {
            return element;
        }
        Statement statement = null;
        if (resource instanceof IRI) {
            IRI s = (IRI) resource;
            Iterator<Vertex> vertices = graph.vertices(s.getLocalName());
            if (vertices.hasNext()) {
                element = vertices.next();
            } else {
                element = graph.addVertex(T.label, "default", T.id, s.getLocalName());
            }
            idMapper.put(resource.stringValue(), element);
        } else {
            statement = (Statement) resource;
            element = createElementFromStatement(graph, statement, idMapper);
        }
        return element;
    }
}
