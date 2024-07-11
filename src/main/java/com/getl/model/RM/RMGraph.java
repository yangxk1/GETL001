package com.getl.model.RM;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class RMGraph {
    //the schemas of the table
    private Map<String, Schema> schemas = new HashMap<>();
    //id(Primary key) -> line
    private Map<String, Line> lines = new HashMap<>();
    private Schema defaultSchema = new Schema("default_table");

    public RMGraph() {
    }

    public void setSchemas(Map<String, Schema> schemas) {
        this.schemas.putAll(schemas);
    }

    public void addSchema(Schema schema) {
        this.schemas.put(schema.getTableName(), schema);
    }
}
