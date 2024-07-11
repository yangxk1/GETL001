package com.getl.query;

import lombok.NonNull;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SPARQLQuery {
    public SPARQLResponse queryRepository(@NonNull Repository repo, @NonNull String query, long timeoutMillis) {
        try (RepositoryConnection con = repo.getConnection()) {

            Query q = con.prepareQuery(QueryLanguage.SPARQL, query);
            q.setMaxExecutionTime((int) Math.ceil(timeoutMillis / 1000f));

            if (q instanceof TupleQuery) {
                return this.performQuery((TupleQuery) q);
            } else if (q instanceof BooleanQuery) {
                return this.performQuery((BooleanQuery) q);
            } else if (q instanceof org.eclipse.rdf4j.query.GraphQuery) {
                return this.performQuery((GraphQuery) q);
            } else {
                throw new RuntimeException("查询方式不支持");
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    private SPARQLResponse performQuery(TupleQuery query) throws QueryEvaluationException {
        Map<String, List<Value>> valueMap = new HashMap<>();
        try (TupleQueryResult result = query.evaluate()) {
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                for (String name : bindingSet.getBindingNames()) {
                    Value value = bindingSet.getValue(name);
                    valueMap.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
                }
            }
        }
        Map<String, List<String>> stringResult = new HashMap<>();
        for (Map.Entry<String, List<Value>> entry : valueMap.entrySet()) {
            String columnName = entry.getKey();
            List<Value> values = entry.getValue();
            List<String> m = values.stream().map(Value::toString).collect(Collectors.toList());
            stringResult.put(columnName, m);
        }
        SPARQLResponse sparqlResponse = new SPARQLResponse();
        sparqlResponse.setType(QueryResponseType.TUPLE);
        sparqlResponse.setTupleResult(stringResult);
        return sparqlResponse;
    }

    private SPARQLResponse performQuery(BooleanQuery query) throws QueryEvaluationException {
        boolean result = query.evaluate();
        SPARQLResponse sparqlResponse = new SPARQLResponse();
        sparqlResponse.setType(QueryResponseType.BOOLEAN);
        sparqlResponse.setBooleanResult(result);
        return sparqlResponse;
    }

    private SPARQLResponse performQuery(GraphQuery query) throws QueryEvaluationException {
        Model m = new LinkedHashModel();
        try (GraphQueryResult result = query.evaluate()) {
            while (result.hasNext()) {
                m.add(result.next());
            }
        }
        SPARQLResponse sparqlResponse = new SPARQLResponse();
        sparqlResponse.setType(QueryResponseType.GRAPH_MODEL);
        sparqlResponse.setGraphResult(m);
        return sparqlResponse;
    }
}
