package com.getl.io;


import com.getl.constant.RdfDataFormat;
import lombok.NonNull;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParser;

import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * Parses RDF files in any of the following RDF Formats:
 * N3, N-Quads, Turtle, Trig and RDF/XML.
 */
public class RDFParser {

    /**
     * Parses an RDF file as a single batch operation.
     * @param stream The input stream of the RDF file.
     * @param format The RDFFormat the file is in.
     * @throws ParserException if an error occurs during the process
     * @return An RDF Model created from the file.
     */
    public static Model parseRDF(@NonNull InputStream stream, @NonNull RDFFormat format) throws ParserException {
        Model model = new LinkedHashModel();
        try (BufferedInputStream bs = new BufferedInputStream(stream)) {
            org.eclipse.rdf4j.rio.RDFParser parser = new NTriplesParser();
//            org.eclipse.rdf4j.rio.RDFParser parser = Rio.createParser(format).set(PRESERVE_BNODE_IDS, false);
            parser.setRDFHandler(new StatementCollector(model));
            parser.parse(bs, "");
        } catch (Exception e) {
            throw new ParserException("Parsing RDF file failed", e);
        }
        return model;
    }

    /**
     * Parses an RDF file as a single batch operation, the format of the RDF data will be inferred,
     * if this is unsuccessful a {@link ParserException} is thrown.
     * @param stream The input stream of the RDF file.
     * @throws ParserException if an error occurs during the process
     * @return An RDF Model created from the file.
     */
    public static Model parseRDF(@NonNull InputStream stream, @NonNull RdfDataFormat format) throws ParserException {
        try {
            RDFFormat rdfFormat = format.toRDFFormat();
            return parseRDF(stream, rdfFormat);
        } catch (IllegalStateException e) {
            throw new ParserException("Parsing RDF file failed, reason: unable to infer rdf format.", e);
        }
    }
}
