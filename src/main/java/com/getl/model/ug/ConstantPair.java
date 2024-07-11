package com.getl.model.ug;

import lombok.Getter;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * type -> value
 * e.g. STRING -> Alice , INTEGER -> 18
 */
public class ConstantPair implements Pair {
    private static final Map<String, Map<Object, ConstantPair>> literalCache = new HashMap<>();

    @Getter
    private String type = "";

    private Comparable value;

    public ConstantPair(String type, Comparable value) {
        this.type = type;
        this.value = value;
    }

    public static ConstantPair getOrCreateConstantPair(String type, Comparable value) {
        Map<Object, ConstantPair> objectLiteralMap = literalCache.computeIfAbsent(type, i -> new HashMap<>());
        return objectLiteralMap.computeIfAbsent(value, i -> new ConstantPair(type, value));
    }

    @Override
    public String serialize() {
        return type + " : " + value;
    }

    @Override
    public String from() {
        return this.type;
    }

    @Override
    public Object to() {
        return this.value;
    }

    @Getter
    public enum LiteralType {
        BYTE("BYTE", Byte.class),
        SHORT("SHORT", Short.class),
        INT("INT", int.class),
        INTEGER("INTEGER", Integer.class),
        DECIMAL("DECIMAL", BigDecimal.class),
        LONG("LONG", Long.class),
        FLOAT("FLOAT", Float.class),
        DOUBLE("DOUBLE", Double.class),
        DATE("DATE", Date.class),
        STRING("STRING", String.class),
        OTHER("OTHER", Object.class);
        private String type;
        private Class aClass;

        LiteralType(String type, Class aClass) {
            this.type = type;
            this.aClass = aClass;
        }

    }

}
