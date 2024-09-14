package com.getl.model.RM;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class RMGraph {
    //the schemas of the table
    private Map<String, Schema> schemas = new HashMap<>();
    //id(Primary key) -> line
    private Map<String, Line> lines = new ConcurrentHashMap<>();
    private Schema defaultSchema = new Schema("default_table");

    public RMGraph() {
    }

    public RMGraph setSchemas(Map<String, Schema> schemas) {
        this.schemas.putAll(schemas);
        return this;
    }

    public void addSchema(Schema schema) {
        this.schemas.put(schema.getTableName(), schema);
    }
}
