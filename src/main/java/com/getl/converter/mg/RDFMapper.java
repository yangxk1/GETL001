package com.getl.converter.mg;

import com.getl.model.MG.MGraph;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.util.HashMap;
import java.util.Map;

public class RDFMapper {
    @Getter
    @Setter
    private MGraph mGraph;

    public RDFMapper(MGraph mGraph) {
        this.mGraph = mGraph;
    }

    public void addRDFModelToMG(@NonNull Model rdfData) {

        Map<Statement, com.getl.model.MG.Statement> rdfToOGStatement = new HashMap<>();

        // Iterate over all RDF statements, create either property or relationship statements in OG.
        for (Statement rdfStatement : rdfData) {
            // Create a copy of the statement without its context/graph.
            Statement rdfStatementNoCTX = SimpleValueFactory.getInstance().createStatement(rdfStatement.getSubject(),
                    rdfStatement.getPredicate(), rdfStatement.getObject());

            // Either obtain an existing OG statement or create a new one.
            com.getl.model.MG.Statement ogStatement = rdfToOGStatement.get(rdfStatementNoCTX);
            if (ogStatement == null) {
                ogStatement = this.createMGStatementFromRDFStatement(rdfStatementNoCTX);
                this.mGraph.add(ogStatement);
                rdfToOGStatement.put(rdfStatementNoCTX, ogStatement);
            }
        }

    }

    private com.getl.model.MG.Statement createMGStatementFromRDFStatement(Statement rdfStatement) {
        Resource subject = rdfStatement.getSubject();
        IRI predicate = rdfStatement.getPredicate();
        Value object = rdfStatement.getObject();
        Resource context = rdfStatement.getContext();
        return new com.getl.model.MG.Statement(subject, predicate, object, context);
    }

    public Model createRDFModelFromMG() {
        Model rdf = new LinkedHashModel();
        for (com.getl.model.MG.Statement statement : this.mGraph) {
            Resource subject = statement.getSubject();
            IRI predicate = statement.getPredicate();
            Value object = statement.getObject();
            Resource context = statement.getContext();
            ValueFactory factory = SimpleValueFactory.getInstance();
            Statement rdfStatement = factory.createStatement(subject, predicate, object, context);
            rdf.add(rdfStatement);
        }
        return rdf;
    }
}