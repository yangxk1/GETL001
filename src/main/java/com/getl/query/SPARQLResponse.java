package com.getl.query;

import lombok.Getter;
import lombok.Setter;
import org.eclipse.rdf4j.model.Model;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class SPARQLResponse {

    private QueryResponseType type;

    private Boolean booleanResult;

    private Map<String, List<String>> tupleResult;

    private Model graphResult;

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (booleanResult != null) {
            str.append("booleanResult:\n");
            str.append(booleanResult);
            return str.toString();
        }
        if (tupleResult != null) {
            str.append("tupleResult:\n");
            tupleResult.forEach((s, strings) -> {
                str.append(s).append(":").append(strings).append("\n");
            });
            return str.toString();
        }
        if (graphResult != null) {
            str.append("graphResult:\n");
            str.append(graphResult);
            return str.toString();
        }
        return str.toString();
    }
}
