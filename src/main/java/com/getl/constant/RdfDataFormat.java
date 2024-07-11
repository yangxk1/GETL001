package com.getl.constant;

import com.fasterxml.jackson.annotation.JsonValue;
import org.eclipse.rdf4j.rio.RDFFormat;

public enum RdfDataFormat {

    GRAPHSON("GRAPHSON"),
    GRAPHML("GRAPHML"),
    NEPTUNECSV("NEPTUNECSV"),
    RDFXML("RDFXML"),
    TURTLE("TURTLE"),
    TRIG("TRIG"),
    NQUADS("NQUADS"),
    NTRIPLES("NTRIPLES");

    private final String underlying;

    RdfDataFormat(String dtString) {
        this.underlying = dtString;
    }

    @JsonValue
    public String getUnderlying() {
        return this.underlying;
    }

    @Override
    public String toString() {
        return underlying;
    }

    public RDFFormat toRDFFormat() throws IllegalStateException {
        if (this == RDFXML) {
            return RDFFormat.RDFXML;
        } else if (this == TURTLE) {
            return RDFFormat.TURTLE;
        } else if (this == TRIG) {
            return RDFFormat.TRIG;
        } else if (this == NQUADS) {
            return RDFFormat.NQUADS;
        } else if (this == NTRIPLES) {
            return RDFFormat.NTRIPLES;
        } else {
            throw new IllegalStateException("Can not convert to RDFFormat; The underlying is not an RDF type");
        }
    }
}
