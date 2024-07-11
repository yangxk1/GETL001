package com.getl.model.RM;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class Line {
    private String tableName;
    private String id;
    private Map<String, Object> values;

    public void addValue(String key, Object value) {
        if (values == null) {
            values = new HashMap<>();
        }
        values.put(key, value);
    }
}
