package com.getl.model.ug;

import com.getl.constant.IRINamespace;
import lombok.Data;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class UnifiedGraph {
    private static volatile AtomicInteger idIncrementer = new AtomicInteger(100);
    private Map<IRI, BasePair> IRIs = new ConcurrentHashMap<>();
    private List<NestedPair> cache = new ArrayList<>();
    private Map<String, IRI> labels = new ConcurrentHashMap<>();
    private Map<String, IRI> s2IRI = new ConcurrentHashMap<>();

    public Collection<NestedPair> getPairs() {
        return cache;
    }

    public void addStatement(NestedPair nestedPair) {
        cache.add(nestedPair);
    }

    public IRI getOrRegisterLabel(String namespace, String label) {
        assert label != null;
        return labels.computeIfAbsent(label, i -> new IRI(namespace, label));
    }

    public IRI getOrRegisterLabel(String labelIRI) {
        assert labelIRI != null;
        return labels.computeIfAbsent(labelIRI, i -> new IRI(IRINamespace.LABEL_NAMESPACE, labelIRI));
    }

    public IRI getOrRegisterPopIRI(String iri) {
        assert iri != null;
        return s2IRI.computeIfAbsent(iri, i -> new IRI(IRINamespace.PROPERTIES_NAMESPACE, iri));
    }

    public IRI getOrRegisterBaseIRI(String namespace,String localName){
        String url = namespace + localName;
        return s2IRI.computeIfAbsent(url, i -> new IRI(namespace, localName));
    }

    public BasePair getOrRegisterBasePair(String iri) {
        assert iri != null;
        return getOrRegisterBasePair("", iri);
    }

    public BasePair getOrRegisterIdIRI(String iri) {
        assert iri != null;
        return getOrRegisterBasePair(IRINamespace.IRI_NAMESPACE, iri);
    }


    public BasePair getOrRegisterBasePair(String baseURI, String IRIId) {
        assert IRIId != null;
        String url = baseURI + IRIId;
        IRI IRIInstant = s2IRI.computeIfAbsent(url, i -> new IRI(baseURI, IRIId));
        return IRIs.computeIfAbsent(IRIInstant, i -> new BasePair(new HashSet<>(), i));
    }

    public void addLabel(BasePair basePair, IRI label) {
        basePair.addLabel(label);
    }

    public BasePair addLabel(IRI subject, IRI label) {
        BasePair basePair = IRIs.computeIfAbsent(subject, i -> new BasePair(new HashSet<>(), i));
        addLabel(basePair, label);
        return basePair;
    }

    public NestedPair add(IRI field, Object id, Pair key, Pair value) {
        NestedPair pair = new NestedPair(field, id, key, value);
        addStatement(pair);
        return pair;
    }

    public NestedPair add(IRI field, Pair key, Pair value) {
        NestedPair pair = new NestedPair(field, key, value);
        addStatement(pair);
        return pair;
    }

    public NestedPair add(BasePair field, Pair key, Pair value) {
        NestedPair pair = new NestedPair(field, key, value);
        addStatement(pair);
        return pair;
    }

    public static Integer getNextID() {
        return idIncrementer.addAndGet(1);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        StringJoiner joiner = new StringJoiner("\n");
        return stringBuilder.toString();
    }

    public void merge(UnifiedGraph source) {
        this.labels.putAll(source.labels);
        this.IRIs.putAll(source.IRIs);
        this.s2IRI.putAll(source.s2IRI);
        this.cache.addAll(source.cache);
    }

    public void gc() {
        this.labels = new HashMap<>();
        this.IRIs = new HashMap<>();
        this.s2IRI = new HashMap<>();
    }

    public void write(PrintWriter pw) {

    }
}
