package com.getl.model.MG;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

import java.util.*;

public class MGraph implements Iterable<Statement> {
    static final Resource[] NULL_CTX = new Resource[]{null};
    Map<String, Statement> IRI2Type;
    Set<Statement> statements;
    private Statement last;
    Iterator<Statement> iterator;

    public MGraph() {
        statements = new HashSet<>();
        IRI2Type = new HashMap<>();
    }

    private Resource[] notNull(Resource[] contexts) {
        if (contexts == null) {
            return new Resource[]{null};
        }
        return contexts;
    }

    public boolean add(Resource subj, IRI pred, Value obj, Resource... contexts) {
        if (subj == null || pred == null || obj == null) {
            throw new UnsupportedOperationException("Incomplete statement");
        }
        Value[] ctxs = notNull(contexts);
        if (ctxs.length == 0) {
            ctxs = NULL_CTX;
        }
        boolean changed = false;
        for (Value ctx : ctxs) {
            Statement st = new Statement(subj, pred, obj, (Resource) ctx);
            changed |= add(st);
        }
        return changed;
    }

    public boolean add(Statement statement) {
        return statements.add(statement);
    }

    public void addAll(Set<Statement> statements) {
        this.statements.addAll(statements);
    }

    public void clear() {
        statements.clear();
    }

    public boolean remove(Statement statement) {
        return statements.remove(statement);
    }

    public int size() {
        return statements.size();
    }

    @Override
    public Iterator<Statement> iterator() {
        return iterator = statements.iterator();
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public Statement next() {
        return last = iterator.next();
    }

    public Statement get(IRI vertexIRI) {
        return IRI2Type.get(vertexIRI.getLocalName());
    }

    public void put(IRI vertexIRI, Statement statement) {
        IRI2Type.put(vertexIRI.getLocalName(), statement);
    }
}
