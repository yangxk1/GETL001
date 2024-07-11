package com.getl.converter;

import com.getl.constant.IRINamespace;
import com.getl.constant.RdfDataFormat;
import com.getl.model.ug.*;
import com.getl.io.ParserException;
import com.getl.io.RDFParser;
import com.getl.model.RDF.LiteralConverter;
import lombok.NonNull;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.io.InputStream;
import java.util.*;

public class RDFConverter {

    private Set<com.getl.model.ug.IRI> typedIRI = new HashSet<>();
    private IRI labelPredicate = RDF.TYPE;
    public UnifiedGraph unifiedGraph;

    public RDFConverter(@NonNull UnifiedGraph UnifiedGraph) {
        this.unifiedGraph = UnifiedGraph;
    }

    public RDFConverter() {
        this.unifiedGraph = new UnifiedGraph();
    }

    /**
     * read RDF file to memory
     */
    public Model readRDF(@NonNull RdfDataFormat rdfDataFormat, @NonNull InputStream stream) throws ParserException {
        return RDFParser.parseRDF(stream, rdfDataFormat);
    }

    /**
     * read RDF file to memory and mapper RDF model to UG
     */
    public Model handleRDF(@NonNull RdfDataFormat rdfDataFormat, @NonNull InputStream stream) throws ParserException {
        Model rdf = RDFParser.parseRDF(stream, rdfDataFormat);
        this.addRDFModelToUG(rdf);
        return rdf;
    }

    /**
     * Adds an RDF data model to the current UnifiedGraph dataset
     *
     * @param rdfData An RDF data model
     */
    public void addRDFModelToUG(@NonNull Model rdfData) {

        Map<Statement, NestedPair> rdfToOGStatement = new HashMap<>();

        // Iterate over all RDF statements, create either property or relationship statements in OG.
        for (Statement rdfStatement : rdfData) {
            // Create a copy of the statement without its context/graph.
            Statement rdfStatementNoCTX = SimpleValueFactory.getInstance().createStatement(rdfStatement.getSubject(),
                    rdfStatement.getPredicate(), rdfStatement.getObject());

            // Either obtain an existing OG statement or create a new one.
            NestedPair ogStatement = rdfToOGStatement.get(rdfStatementNoCTX);
            if (ogStatement == null) {
                ogStatement = this.createPairFromRDFStatement(rdfStatementNoCTX);
                if (ogStatement == null) {
                    continue;
                }
                this.unifiedGraph.addStatement(ogStatement);
                rdfToOGStatement.put(rdfStatementNoCTX, ogStatement);
            }
        }
    }

    /**
     * handle one RDF statement, mapper it to one UG pair
     */
    private NestedPair createPairFromRDFStatement(Statement statement) {
        final Resource subject = statement.getSubject();
        final IRI predicate = statement.getPredicate();
        final Value object = statement.getObject();

        BasePair subjectSN;
        if (labelPredicate.equals(predicate)) {
            com.getl.model.ug.IRI objectSN;
            assert subject instanceof IRI && object instanceof IRI;
            subjectSN = this.createOrObtainSimpleNode(subject);
            objectSN = this.createOrObtainLabelNode(object);
            this.unifiedGraph.addLabel(subjectSN, objectSN);
            return null;
        }
        com.getl.model.ug.IRI predicateSN = this.createOrObtainLabelNode(predicate);
        subjectSN = this.createOrObtainSimpleNode(subject);

        if (object instanceof Literal) {
            Literal objLit = (Literal) object;
            return this.unifiedGraph.add(predicateSN, subjectSN, LiteralConverter.convertToKVLiteral(objLit));
        }
        Pair objectSN;
        if (object instanceof IRI) {
            objectSN = this.createOrObtainSimpleNode((IRI) object);
        } else {
            objectSN = this.createOrObtainSimpleNode((BNode) object);
        }
        return this.unifiedGraph.add(predicateSN, subjectSN, objectSN);
    }

    /**
     * mapper UG to RDF model
     */
    public Model createRDFModelFromUG() {
        Model rdf = new LinkedHashModel();
        Map<String, Statement> resolvedStatement = new HashMap<>();
        for (NestedPair nestedPair : this.unifiedGraph.getPairs()) {
            Optional<Statement> rdfStatement = this.transformToRDFStatement(nestedPair, rdf, resolvedStatement);
            rdfStatement.ifPresent(rdf::add);
        }
        //handle all basePair. Prevent IRIs from having no properties or relationships
        for (BasePair basePair : this.unifiedGraph.getIRIs().values()) {
            addType(rdf, basePair);
        }
        return rdf;
    }

    private void addType(Model rdf, BasePair basePair) {
        if (basePair == null) {
            return;
        }
        com.getl.model.ug.IRI keyIRI = basePair.to();
        if (typedIRI.contains(keyIRI)) {
            return;
        }
        typedIRI.add(keyIRI);
        basePair.from().forEach(label -> {
            if (label != null) {
                IRI key = SimpleValueFactory.getInstance().createIRI(keyIRI.getNameSpace(), keyIRI.getLocalName());
                IRI typeIRI = SimpleValueFactory.getInstance().createIRI(label.getNameSpace(), label.getLocalName());
                Optional.ofNullable(createRDFStatement(key, labelPredicate, typeIRI)).ifPresent(rdf::add);

            }
        });
    }

    /**
     * Using RDF Reification to express the properties of statement
     * <a href="https://stackoverflow.com/questions/55885944/can-rdf-model-a-labeled-property-graph-with-edge-properties">介绍</a>
     */
    private IRI createStatementIRI(NestedPair nestedPair, Model rdf, Map<String, Statement> resolvedStatement) {
        ValueFactory factory = SimpleValueFactory.getInstance();
        IRI iri = factory.createIRI(IRINamespace.STATEMENT_NAMESPACE, nestedPair.getID());
        rdf.add(createRDFStatement(iri, labelPredicate, RDF.STATEMENT));
        Optional<Statement> statement = transformToRDFStatement(nestedPair, rdf, resolvedStatement);
        Statement RDFStatement = statement.orElse(null);
        assert RDFStatement != null;
        rdf.add(createRDFStatement(iri, RDF.SUBJECT, RDFStatement.getSubject()));
        rdf.add(createRDFStatement(iri, RDF.PREDICATE, RDFStatement.getPredicate()));
        rdf.add(createRDFStatement(iri, RDF.OBJECT, RDFStatement.getObject()));
        return iri;
    }

    /**
     * mapper UG pair to RDF statement
     * (predicate -> id) -> (subject -> object)
     */
    private Optional<Statement> transformToRDFStatement(NestedPair nestedPair, Model rdf, Map<String, Statement> resolvedStatement) {
        Statement statement = resolvedStatement.get(nestedPair.getID());
        if (statement != null) {
            return Optional.of(statement);
        }
        final Optional<Resource> subject;
        final Optional<IRI> predicate;
        final Optional<Resource> object;

        Set<com.getl.model.ug.IRI> fields = nestedPair.from().from();
        //edge and property only have one label
        com.getl.model.ug.IRI field = fields.iterator().next();
        predicate = Optional.of(SimpleValueFactory.getInstance().createIRI(field.getNameSpace(), field.getLocalName()));
        //subject
        Pair key = nestedPair.to().from();
        if (key instanceof NestedPair) {
            subject = Optional.of(createStatementIRI((NestedPair) key, rdf, resolvedStatement));
        } else {
            BasePair basePair = (BasePair) key;
            addType(rdf, basePair);
            subject = createNode(basePair);
        }
        //object
        Pair value = nestedPair.to().to();
        if (value instanceof ConstantPair) {
            //literal object (a property)
            Object objectValue = value.to();
            Literal LiteralObject = LiteralConverter.convertToLiteral(objectValue);
            if (subject.isEmpty()) {
                return Optional.empty();
            }
            statement = createRDFStatement(subject.get(), predicate.get(), LiteralObject);
            resolvedStatement.put(nestedPair.getID(), statement);
            return Optional.of(statement);
        }
        if (value instanceof NestedPair) {
            object = Optional.of(createStatementIRI((NestedPair) value, rdf, resolvedStatement));
        } else {
            BasePair basePair = (BasePair) value;
            addType(rdf, basePair);
            object = createNode(basePair);
        }
        if (subject.isEmpty() || object.isEmpty()) {
            return Optional.empty();
        }
        statement = createRDFStatement(subject.get(), predicate.get(), object.get());
        resolvedStatement.put(nestedPair.getID(), statement);
        return Optional.of(statement);
    }

    private Statement createRDFStatement(Resource subject, IRI predicate, Value object) {
        ValueFactory factory = SimpleValueFactory.getInstance();
        return factory.createStatement(subject, predicate, object);
    }

    private Optional<Resource> createNode(BasePair basePair) {
        if (basePair == null) {
            return Optional.empty();
        }
        if (basePair.hasLabel(IRINamespace.BLANK_NODE)) {
            return Optional.of(SimpleValueFactory.getInstance().createBNode(basePair.to().getLocalName()));
        }
        return Optional.of(SimpleValueFactory.getInstance().createIRI(basePair.to().getNameSpace(), basePair.to().getLocalName()));
    }

    public void labelPredicate(String url) {
        labelPredicate = SimpleValueFactory.getInstance().createIRI(url);
    }

    public BasePair createOrObtainSimpleNode(Resource predicate) {
        if (predicate instanceof BNode) {
            BNode bNode = (BNode) predicate;
            BasePair orRegisterIRI = unifiedGraph.getOrRegisterIdIRI(bNode.getID());
            unifiedGraph.addLabel(orRegisterIRI, IRINamespace.BLANK_NODE);
            return orRegisterIRI;
        } else if (predicate instanceof org.eclipse.rdf4j.model.IRI) {
            org.eclipse.rdf4j.model.IRI IRI = (org.eclipse.rdf4j.model.IRI) predicate;
            return unifiedGraph.getOrRegisterBasePair(IRI.getNamespace(), IRI.getLocalName());
        }
        return unifiedGraph.getOrRegisterBasePair(predicate.stringValue());
    }

    public com.getl.model.ug.IRI createOrObtainLabelNode(Value object) {
        return unifiedGraph.getOrRegisterLabel(object.stringValue());
    }
}
