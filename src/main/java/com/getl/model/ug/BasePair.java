package com.getl.model.ug;

import lombok.Setter;

import java.util.Set;

/**
 * label -> id
 * e.g. Person -> ps_001 , Knows -> kn_001 , rdf:person -> rdf:001
 */
public class BasePair implements Pair {

    @Setter
    private Set<IRI> labels;
    @Setter
    private IRI valueIRI;

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
}
