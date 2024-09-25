package com.getl.model.RM;

import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Data
public class Schema {
    //Primary key column name
    public static final String KEY = "id";
    //Data in which table , and it's the label of elements
    public static final String TABLE_NAME = "table_name";
    public static final String TYPE = "type";
    public static final String COLUMNS = "columns";
    public static final String IN = "_in";
    public static final String OUT = "_out";
    public static final String IN_LABEL = "_in_label";
    public static final String OUT_LABEL = "_out_label";
    public static final String HAS_ID = "has_id";
    //table name(label)
    private String tableName;
    //0:node\1:edge
    private int type = 0;
    //0:has id
    private int hasId = 0;
    //if this is a schema for edges, it has inVertex id(id) and outVertex id(out)
    private String in;
    private String out;
    private String inLabel;
    private String outLabel;
    //property name and its type
    private Map<String, String> columns = new HashMap<>();

    public void setType(int type) {
        if (type == 1) {
            if (StringUtils.isBlank(in)) {
                in = "in_node";
                inLabel = "in_node";
            }
            if (StringUtils.isBlank(out)) {
                out = "out_node";
                outLabel = "out_node";
            }
        }
        this.type = type;
    }
    public Schema(String tableName, String out,String outLabel, String in ,String inLabel) {
        this.tableName = tableName;
        this.in = in;
        this.inLabel = inLabel;
        this.out = out;
        this.outLabel = outLabel;
        setType(1);
    }

    public Schema(String tableName, String out, String in) {
        this.tableName = tableName;
        this.in = in;
        this.inLabel = in;
        this.out = out;
        this.outLabel = out;
        setType(1);
    }

    public Schema(String tableName) {
        this.tableName = tableName;
        this.in = in;
        this.out = out;
    }

    public boolean isNode() {
        return type == 0;
    }

    public boolean isNotNode() {
        return !isNode();
    }

    public Schema addColumn(String key, String type) {
        this.columns.put(key, type);
        return this;
    }

    public void setColumns(Map<String, String> columns) {
        this.columns.putAll(columns);
    }

    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String NUMERIC = "DOUBLE";
    public static final String SMALL_TEXT = "VARCHAR(127)";
    public static final String MID_TEXT = "VARCHAR(255)";
    public static final String LARGE_TEXT = "VARCHAR(512)";
    public static final String MID_LARGE_TEXT = "VARCHAR(1024)";
    public static final String HUGE_TEXT = "VARCHAR(2048)";
    public static final String VERY_HUGE_TEXT = "VARCHAR(10240)";
    public static final String DATE = "DATE";
    public static final String DATATYPE_PK = INT;
    public static final String DATATYPE_SL = INT;
    public static final String DATATYPE_IL = INT;
    public static final String SMALL_JSON = MID_TEXT;
    public static final String LARGE_JSON = LARGE_TEXT;
}
