package com.getl.model.LPG;

import cn.hutool.core.collection.CollectionUtil;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

/**
 * The parent class of all elements in the label property graph
 * It's different from common label property graphs, where nodes, relationships, and properties can all have property and relationships
 */
public abstract class LPGElement implements Vertex {
    public static final String LABEL_SPLITTER = "___";
    @Setter
    private Object id;
    @Getter
    private Map<String, LPGSuperVertex> superVertexes = new HashMap<>();
    @Getter
    private Set<String> superVertexLabels = new HashSet<>();
    @Getter
    private final LPGGraph graph;
    // Maps property names to property objects
    protected final Map<String, LPGProperty> propNameToProp = new HashMap<>();

    private boolean removed = false;

    protected LPGElement(LPGGraph graph) {
        this.graph = graph;
    }

    /**
     * Either creates a new property in the element with given value, or adds
     * the value to an already existing property.
     *
     * @param propName  The name of the property
     * @param propValue The value of the property
     */
    public LPGProperty addPropertyValue(@NonNull String propName, @NonNull Object propValue) {
        LPGProperty property;
        if (this.propNameToProp.containsKey(propName)) {
            property = this.propNameToProp.get(propName);
            property.addValue(propValue);
            return property;
        } else {
            property = new LPGProperty(this.graph, propName, this, propValue);
            this.propNameToProp.put(propName, property);
        }
        return property;
    }

    /**
     * Adds a property to this element
     *
     * @param prop The property to add.
     */
    public void addProperty(@NonNull LPGProperty prop) {
        this.addPropertyValue(prop.name, prop.value);
    }

    /**
     * this element -> outEdge
     */
    private final LinkedHashSet<LPGEdge> outEdges = new LinkedHashSet<>();
    /**
     * inEdge -> this element
     */
    private final LinkedHashSet<LPGEdge> inEdges = new LinkedHashSet<>();

    /**
     * Adds a new edge to this vertex
     *
     * @param edge The edge
     */
    public void addEdge(@NonNull LPGEdge edge) {
        //outV -> edge -> inV
        if (edge.inVertex == this) {
            this.inEdges.add(edge);
        } else {
            this.outEdges.add(edge);
        }
    }

    /**
     * Returns the property with given name or empty optional if not prop of that name present
     *
     * @param name The name of the property
     * @return The property or empty optional if not found.
     */
    public Optional<LPGProperty> propertyForName(@NonNull String name) {
        return Optional.ofNullable(propNameToProp.get(name));
    }

    /**
     * Gets the id of this element.
     *
     * @return The id, if no id is set prior the id will be the hashCode of this element.
     */
    public Object getId() {
        if (id == null) {
            return this.hashCode();
        }
        return id;
    }

    /**
     * Gets all properties of this element
     *
     * @return The properties of this element.
     */
    public Collection<LPGProperty> getProperties() {
        return propNameToProp.values();
    }

    public LinkedHashSet<LPGEdge> getOutEdges() {
        return outEdges;
    }

    public LinkedHashSet<LPGEdge> getInEdges() {
        return inEdges;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        LPGVertex inVertexL = (LPGVertex) inVertex;
        return new LPGEdge(this.graph, this, inVertexL, label);
    }

    /**
     * add a property for this element
     *
     * @param cardinality the desired cardinality of the property key
     * @param key         the name of the vertex property
     * @param value       The value of the vertex property
     * @param keyValues   the key/value pairs to turn into vertex property properties
     * @return the newly created vertex property
     */
    @Override
    public <V> LPGProperty property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        LPGProperty property = this.addPropertyValue(key, value);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        optionalId.ifPresent(property::setId);
        ElementHelper.attachProperties(property, keyValues);
        return property;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        Set<String> hashSet = new HashSet<>(List.of(edgeLabels));
        boolean jmp = CollectionUtil.isEmpty(hashSet);
        List<Edge> edges = new ArrayList<>();
        if (Direction.OUT.equals(direction) || Direction.BOTH.equals(direction)) {
            this.outEdges.stream()
                    .filter(i -> jmp || hashSet.contains(i.label()))
                    .map(i -> (Edge) i)
                    .forEach(edges::add);
        }
        if (Direction.IN.equals(direction) || Direction.BOTH.equals(direction)) {
            this.inEdges.stream()
                    .filter(i -> jmp || hashSet.contains(i.label()))
                    .map(i -> (Edge) i)
                    .forEach(edges::add);
        }
        return edges.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        Iterator<Edge> edges = this.edges(direction, edgeLabels);
        ArrayList<Vertex> vertices = new ArrayList<>();
        while (edges.hasNext()) {
            Edge next = edges.next();
            Vertex other = next.inVertex() == this ? next.outVertex() : next.inVertex();
            vertices.add(other);
        }
        return vertices.iterator();
    }

    @Override
    public Object id() {
        return getId();
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    protected void removed(boolean removed) {
        this.removed = removed;
    }

    public boolean isRemoved() {
        return this.removed;
    }

    @Override
    public void remove() {
        this.removed(true);
        /*
         * Call by tx or by graph to remove vertex,
         * call by tx if the vertex is new because the context is dependent
         */
        //TODO Transaction
    }

    @Override
    public Iterator<LPGProperty> properties(String... propertyKeys) {
        int propsCapacity = propertyKeys.length == 0 ?
                this.propNameToProp.size() :
                propertyKeys.length;
        List<LPGProperty> props = new ArrayList<>(propsCapacity);

        if (propertyKeys.length == 0) {
            props.addAll(this.getProperties());
        } else {
            for (String key : propertyKeys) {
                Optional<LPGProperty> prop = this.propertyForName(key);
                prop.ifPresent(props::add);
            }
        }
        return props.iterator();
    }

    public void addProperty(Map<String, Object> pop) {
        pop.forEach(this::addPropertyValue);
    }

    public LPGSuperVertex inSuperVertex(String label) {
        return superVertexes.get(label);
    }

}
