package com.getl.model.LPG;

import lombok.Getter;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A property has a property name and zero or more property values.
 */
public class LPGProperty extends LPGElement implements VertexProperty {

    /**
     * The element that owns this VertexProperty.
     */
    public final LPGElement element;
    /**
     * The name of this property
     */
    public final String name;
    /**
     * The value of this property
     */
    @Getter
    public Object value;
    /**
     * Creates a new property with given name
     *
     * @param name The name of property
     */
    public LPGProperty(LPGGraph graph, @NonNull String name, LPGElement element) {
        super(graph);
        this.element = element;
        this.name = name;
    }

    /**
     * Creates a new property with given name and values
     *
     * @param name   The name of property
     * @param values The values of property
     */
    public LPGProperty(LPGGraph graph, @NonNull String name, LPGElement element, Object... values) {
        super(graph);
        this.name = name;
        this.element = element;
        for (Object value : values) {
            this.addValue(value);
        }
    }

    /**
     * Creates a new property with given name and values
     *
     * @param name   The name
     * @param values The values
     */
    public LPGProperty(LPGGraph graph, @NonNull String name, LPGElement element, Collection<?> values) {
        super(graph);
        this.name = name;
        this.element = element;
        for (Object value : values) {
            this.addValue(value);
        }
    }

    /**
     * Adds a value to this property
     *
     * @param value The value
     */
    public void addValue(@NonNull Object value) {
        if (this.value == null) {
            this.value = value;
            return;
        }
        if (this.value instanceof List) {
            ((List<Object>) this.value).add(value);
            return;
        }
        List<Object> values = new ArrayList<>();
        values.add(this.value);
        values.add(value);
        this.value = values;
    }

    @Override
    public String toString() {
        return name + " : " + value;
    }

    @Override
    public String key() {
        return this.name;
    }

    @Override
    public Object value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public Vertex element() {
        return this.element;
    }

    @Override
    public String label() {
        return name;
    }

}
