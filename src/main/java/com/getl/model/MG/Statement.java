package com.getl.model.MG;

import lombok.Getter;
import lombok.Setter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

/**
 * The id of Statement is the Memory reference of this Object
 */
@Setter
public class Statement implements org.eclipse.rdf4j.model.Statement, Resource {
    private Resource subject;
    private IRI predicate;
    private Value object;
    private Resource context;
    @Getter
    @Setter
    private Object id;

    public Statement(Resource subject, IRI predicate, Value object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public Statement(Resource subject, IRI predicate, Value object, Resource context) {
        this(subject, predicate, object);
        this.context = context;
    }

    @Override
    public Resource getSubject() {
        return this.subject;
    }

    @Override
    public IRI getPredicate() {
        return this.predicate;
    }

    @Override
    public Value getObject() {
        return this.object;
    }

    @Override
    public Resource getContext() {
        return this.context;
    }

    @Override
    public String stringValue() {
        return this.toString();
    }
}
