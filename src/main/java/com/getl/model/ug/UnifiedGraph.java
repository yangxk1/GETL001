package com.getl.model.ug;

import com.getl.constant.IRINamespace;
import com.getl.converter.PropertiesGraphConfig;
import lombok.Data;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Data
public class UnifiedGraph implements Graph {
    public Map<String, PropertiesGraphConfig> lpgConfigs;
    private static volatile AtomicInteger idIncrementer = new AtomicInteger(100);
    private Map<IRI, BasePair> IRI2BasePair = new ConcurrentHashMap<>();
    private List<NestedPair> cache = new ArrayList<>();
    private Map<String, IRI> labels = new ConcurrentHashMap<>();
    private Map<String, IRI> s2IRI = new ConcurrentHashMap<>();
    private NamespacePool nsPool = NamespacePool.getInstance();

    public Collection<NestedPair> getPairs() {
        return cache;
    }

    public void addStatement(NestedPair nestedPair) {
        cache.add(nestedPair);
    }

    public IRI getOrRegisterLabel(String namespace, String label) {
        assert label != null;
        String namespaceId = nsPool.getNamespaceId(namespace);
        return labels.computeIfAbsent(label, i -> new IRI(namespaceId, label));
    }

    public IRI getOrRegisterEdgeLabel(String label) {
        return labels.computeIfAbsent(label, i -> new IRI(IRINamespace.EDGE_NAMESPACE_ID, label));
    }

    public IRI getOrRegisterLabel(String labelIRI) {
        assert labelIRI != null;
        return labels.computeIfAbsent(labelIRI, i -> new IRI(IRINamespace.LABEL_NAMESPACE_ID, labelIRI));
    }

    public IRI getOrRegisterPopIRI(String propertyName) {
        assert propertyName != null;
        return s2IRI.computeIfAbsent(propertyName, i -> new IRI(IRINamespace.PROPERTIES_NAMESPACE_ID, propertyName));
    }

    public IRI getOrRegisterIDIRI(String localName) {
        String namespaceId = IRINamespace.IRI_NAMESPACE_ID;
        String url = namespaceId + "$$" + localName;
        return s2IRI.computeIfAbsent(url, i -> new IRI(IRINamespace.IRI_NAMESPACE_ID, localName));
    }

    public IRI getOrRegisterBaseIRI(String namespace, String localName) {
        String namespaceId = nsPool.getNamespaceId(namespace);
        String url = namespaceId + "$$" + localName;
        return s2IRI.computeIfAbsent(url, i -> new IRI(namespaceId, localName));
    }

    public BasePair getOrRegisterBasePair(String iri) {
        assert iri != null;
        return getOrRegisterBasePair("", iri);
    }

    public BasePair getOrRegisterIdIRI(String iri) {
        assert iri != null;
        return getOrRegisterBasePair(IRINamespace.IRI_NAMESPACE_ID, iri);
    }


    public BasePair getOrRegisterBasePair(String baseURI, String IRIId) {
        assert IRIId != null;
        String url = baseURI + IRIId;
        IRI IRIInstant = getOrRegisterBaseIRI(baseURI, IRIId);
        return IRI2BasePair.computeIfAbsent(IRIInstant, i -> new BasePair(new HashSet<>(), i));
    }

    public void addLabel(BasePair basePair, IRI label) {
        basePair.addLabel(label);
    }

    public BasePair addLabel(IRI subject, IRI label) {
        BasePair basePair = IRI2BasePair.computeIfAbsent(subject, i -> new BasePair(new HashSet<>(), i));
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

    public NestedPair add(BasePair field, BasePair key, Pair value) {
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
        this.IRI2BasePair.putAll(source.IRI2BasePair);
        this.s2IRI.putAll(source.s2IRI);
        this.cache.addAll(source.cache);
    }

    public void gc() {
        this.labels = new HashMap<>();
        this.IRI2BasePair = new HashMap<>();
        this.s2IRI = new HashMap<>();
    }

    public void write(PrintWriter pw) {

    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return null;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        Iterator basePairIterator = this.IRI2BasePair.values().stream().filter(basePair -> basePair.getLabels().stream().map(IRI::getLocalName).collect(Collectors.toList()).contains(IRINamespace.LABEL_NAMESPACE_ID)).iterator();
        return basePairIterator;
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return null;
    }

    @Override
    public Transaction tx() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return null;
    }
}
