package com.getl.model.ug;


import com.getl.constant.IRINamespace;

import java.util.Set;

import static com.getl.model.ug.UnifiedGraph.getNextID;

/**
 * (predicate -> id) -> (inIRI -> outIRI)
 * e.g. (name -> nm_01) -> (ps_001 -> Alice)
 * e.g. (knows -> kn_01) -> (ps_001 -> ps_002)
 */
public class NestedPair implements Pair {

    private final BasePair predicate;
    private final RelationPair relationPair;

    public NestedPair(BasePair predicate, Pair key, Pair value) {
        this.relationPair = new RelationPair(key, value);
        this.predicate = predicate;
    }

    public NestedPair(IRI predicateLabel, Pair key, Pair value) {
        this.relationPair = new RelationPair(key, value);
        IRI iri = new IRI(IRINamespace.IRI_NAMESPACE, "AUTO:" + getNextID());
        this.predicate = new BasePair(Set.of(predicateLabel), iri);
    }

    public NestedPair(IRI predicateLabel, Object id, Pair key, Pair value) {
        this.relationPair = new RelationPair(key, value);
        IRI iri = new IRI(IRINamespace.IRI_NAMESPACE, id.toString());
        this.predicate = new BasePair(Set.of(predicateLabel), iri);
    }

    @Override
    public String serialize() {
        return "( " + predicate + " ) ->( " + relationPair + " )";
    }

    @Override
    public BasePair from() {
        return this.predicate;
    }

    @Override
    public RelationPair to() {
        return this.relationPair;
    }

    public String getID() {
        return predicate.to().getLocalName();
    }


    public static class RelationPair implements Pair {

        private final Pair from;
        private final Pair to;

        public RelationPair(Pair from, Pair to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public String serialize() {
            return this.from + " -> " + this.to;
        }

        @Override
        public Pair from() {
            return this.from;
        }

        @Override
        public Pair to() {
            return this.to;
        }
    }

}
