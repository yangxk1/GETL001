package com.getl.model.ug;

import com.getl.constant.IRINamespace;
import com.getl.model.LPG.LPGElement;
import com.getl.model.LPG.LPGProperty;
import lombok.Getter;
import lombok.Setter;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;
import java.util.stream.Collectors;

import static com.getl.model.LPG.LPGElement.LABEL_SPLITTER;

/**
 * label -> id
 * e.g. Person -> ps_001 , Knows -> kn_001 , rdf:person -> rdf:001
 */
//TODO UGM 2 RM and PG
public class BasePair implements Pair, Vertex, Edge {

    @Setter
    @Getter
    private Set<IRI> labels;
    @Setter
    @Getter
    private IRI valueIRI;

    @Setter
    @Getter
    private List<NestedPair> relations = new ArrayList<>();

    @Setter
    @Getter
    private NestedPair content;

    public BasePair(Set<IRI> labels, IRI valueIRI) {
        this.labels = labels;
        this.valueIRI = valueIRI;
    }

    public void addLabel(IRI label) {
        this.labels.add(label);
    }

    @Override
    public String serialize() {
        return String.join(",", labels) + " : " + valueIRI;
    }

    @Override
    public Set<IRI> from() {
        return labels;
    }

    @Override
    public IRI to() {
        return valueIRI;
    }

    public boolean hasLabel(IRI label) {
        return labels != null && labels.contains(label);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (direction == Direction.BOTH || direction == Direction.IN) {
            throw new RuntimeException("UN SUPPORT IN");
        }
        ArrayList<Vertex> vertices = new ArrayList<>();
        vertices.add((BasePair) this.content.to().to());
        return vertices.iterator();
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        Set<String> hashSet = new HashSet<>(List.of(edgeLabels));
        if (direction == Direction.BOTH || direction == Direction.IN) {
            throw new RuntimeException("UN SUPPORT IN");
        }
        Iterator basePairIterator;
        basePairIterator = relations.stream().filter(pair -> {
            if (hashSet.isEmpty()) {
                return IRINamespace.EDGE_NAMESPACE.equals(pair.from().getLabels().stream().findFirst().map(IRI::getNameSpace).orElse(""));
            } else {
                return hashSet.contains(pair.from().label());
            }
        }).map(NestedPair::from).filter(Objects::nonNull).iterator();
        return basePairIterator;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        //for vertex
        Iterator<Edge> edges = edges(direction, edgeLabels);
        ArrayList<Vertex> vertices = new ArrayList<>();
        while (edges.hasNext()) {
            BasePair basePair = (BasePair) ((BasePair) edges.next()).content.to().to();
            vertices.add(basePair);
        }
        return vertices.iterator();
    }

    @Override
    public Object id() {
        return valueIRI.getLocalName();
    }

    @Override
    public String label() {
        return this.labels.stream().map(IRI::toString).collect(Collectors.joining(LABEL_SPLITTER));
    }

    @Override
    public Graph graph() {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public Iterator<LPGProperty> properties(String... propertyKeys) {
        Set<String> hashSet = new HashSet<>(List.of(propertyKeys));
        return relations.stream().map(pair -> {
            String label = pair.from().labels.stream().findFirst().map(IRI::getNameSpace).orElse("");
            if ((propertyKeys.length == 0 && !IRINamespace.PROPERTIES_NAMESPACE.equals(label)) || hashSet.contains(label)) {
                return null;
            }
            Object value = Optional.of(pair).map(NestedPair::to).map(NestedPair.RelationPair::to).map(Pair::to).orElse(null);
            if (value == null) {
                return null;
            }
            return new LPGProperty(null, label, null, value);
        }).filter(Objects::nonNull).collect(Collectors.toList()).iterator();
    }

    @Override
    public String toString() {
        return "BasePair{" +
                "labels=" + labels +
                ", valueIRI=" + valueIRI +
                "}\n";
    }
}
