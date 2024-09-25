package com.getl.model.RM;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.TypeReference;
import cn.hutool.json.JSONUtil;
import com.getl.converter.async.AsyncRM2UMG;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.getl.model.ug.UnifiedGraph;
import org.apache.commons.lang.StringUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class MysqlOp {

    public static AsyncRM2UMG asyncRM2UMG;

    public static void createSchema(MysqlSessions session) {

        String sql = "CREATE TABLE IF NOT EXISTS SESSION (" + Schema.TABLE_NAME + " varchar(255)," + Schema.TYPE + " int,"
                + Schema.HAS_ID + " int," + Schema.COLUMNS + " json," + Schema.IN_LABEL + " varchar(255)," + Schema.OUT_LABEL + " varchar(255),"
                + Schema.IN + " varchar(255)," + Schema.OUT + " varchar(255), PRIMARY KEY (" + Schema.TABLE_NAME + "))";
        try {
            session.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create table :" + sql, e);
        }
    }

    private static void createTable(MysqlSessions session, Schema table) {
        StringBuilder dropSQL = new StringBuilder();
        dropSQL.append("Drop table IF EXISTS `").append(table.getTableName()).append("`;");
        try {
            session.execute(dropSQL.toString());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to DROP table", e);
        }
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS `");
        sql.append(table.getTableName()).append("` ( id VARCHAR(255),");
        // Add columns
        for (Map.Entry<String, String> entry :
                table.getColumns().entrySet()) {
            sql.append(entry.getKey());
            sql.append(" ");
            sql.append(entry.getValue());
            sql.append(", ");
        }
        if (!table.isNode()) {
            sql.append(table.getOut());
            sql.append(" VARCHAR(255), ");
            sql.append(table.getIn());
            sql.append(" VARCHAR(255), ");
        }
        // Specified primary keys
        sql.append("PRIMARY KEY (id))");
        sql.append(";");

        try {
            session.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create table", e);
        }
    }

    @Deprecated
    public static void writeSchema(MysqlSessions session, RMGraph graph) {
        for (Schema schema : graph.getSchemas().values()) {
            //update SCHEMA
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO SESSION (").append(Schema.TABLE_NAME).append(",")
                    .append(Schema.TYPE).append(",")
                    .append(Schema.COLUMNS).append(",")
                    .append(Schema.IN).append(",")
                    .append(Schema.OUT).append(") VALUES ('");
            sql.append(schema.getTableName()).append("',");
            sql.append(schema.getType()).append(",'");
            sql.append(JSONUtil.toJsonStr(schema.getColumns())).append("','");
            sql.append(schema.getIn()).append("','");
            sql.append(schema.getOut());
            sql.append("') ON DUPLICATE KEY UPDATE table_name = VALUES(table_name) , type = VALUES(type) , columns = VALUES(columns), _in = VALUES(_in), _out = VALUES(_out);");
            try {
                session.execute(sql.toString());
            } catch (SQLException e) {
                System.out.println(e.toString());
                throw new RuntimeException("Failed to create table");
            }
        }
    }

    public static void write(MysqlSessions session, RMGraph graph) {
        Collection<Schema> schemas = graph.getSchemas().values();
        for (Schema schema : schemas) {
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO SESSION (").append(Schema.TABLE_NAME).append(",")
                    .append(Schema.TYPE).append(",")
                    .append(Schema.HAS_ID).append(",")
                    .append(Schema.COLUMNS).append(",")
                    .append(Schema.IN_LABEL).append(",")
                    .append(Schema.OUT_LABEL).append(",")
                    .append(Schema.IN).append(",")
                    .append(Schema.OUT).append(") VALUES ('");
            sql.append(schema.getTableName()).append("',");
            sql.append(schema.getType()).append(",");
            sql.append(schema.getHasId()).append(",'");
            sql.append(JSONUtil.toJsonStr(schema.getColumns())).append("','");
            sql.append(schema.getInLabel()).append("','");
            sql.append(schema.getOutLabel()).append("','");
            sql.append(schema.getIn()).append("','");
            sql.append(schema.getOut());
            sql.append("') ON DUPLICATE KEY UPDATE table_name = VALUES(table_name) , type = VALUES(type) , has_id = VALUES(has_id) , columns = VALUES(columns), _in_label=VALUES(_in_label) , _out_label=VALUES(_out_label) , _in = VALUES(_in), _out = VALUES(_out);");
            try {
                session.execute(sql.toString());
            } catch (SQLException e) {
                System.out.println(e.toString());
                throw new RuntimeException("Failed to create table");
            }
            createTable(session, schema);
        }
        int c = 0, j = 0;
        Collection<Line> allLines = graph.getLines().values();
        Multimap<String, Line> multimap = group(allLines);
        for (String label : multimap.keySet()) {
            Collection<Line> lines = multimap.get(label);
            Schema schema = graph.getSchemas().get(label);
            if (schema == null) {
                continue;
            }
            StringBuilder sqlHead = new StringBuilder();
            sqlHead.append("INSERT INTO `").append(label).append("` (");
            sqlHead.append("id");
            ArrayList<String> columns = new ArrayList<>(schema.getColumns().keySet());
            if (schema.isNotNode()) {
                columns.add(schema.getIn());
                columns.add(schema.getOut());
            }
            for (String key : columns) {
                sqlHead.append(" , ");
                sqlHead.append(key);
            }
            sqlHead.append(") VALUES ");
            StringBuilder sqlTail = new StringBuilder();
            sqlTail.append("ON DUPLICATE KEY UPDATE ");
            int i = 0;
            for (String key : columns) {
                if (i != 0) {
                    sqlTail.append(" , ");
                }
                sqlTail.append(key).append(" = VALUES(");
                sqlTail.append(key).append(")");
                i++;
            }
            sqlTail.append(";");
            boolean f = false;
            c = 0;
            StringBuilder sql = new StringBuilder();
            for (Line line : lines) {
                if (c == 0) {
                    sql = new StringBuilder();
                    sql.append(sqlHead);
                    f = false;
                }
                c++;
                j++;
                if (f) {
                    sql.append(",");
                }
                f = true;
                sql.append("('").append(line.getId()).append("'");
                for (String key : columns) {
                    sql.append(" , ");
                    Object value = line.getValues().get(key);
                    if (value instanceof String) {
                        value = "'" + ((String) value).replace("\\", "\\\\").replace("'", "\\'") + "'";
                    } else if (value instanceof Date) {
                        value = "'" + DateUtil.format((Date) value, "yyyy-MM-dd") + "'";
                    } else if (null != value) {
                        value = "'" + value.toString().replace("\\", "\\\\").replace("'", "\\'") + "'";
                    }
                    sql.append(value);
                }
                sql.append(")\n");
                if (c == 32768) {
                    c = 0;
                    sql.append(sqlTail);
                    try {
                        session.execute(sql.toString());
                    } catch (SQLException e) {
                        FileOutputStream urgOut = null;
                        try {
                            urgOut = new FileOutputStream("XXX");
                        } catch (FileNotFoundException fileNotFoundException) {
                            throw new RuntimeException(fileNotFoundException);
                        }
                        PrintWriter pw = new PrintWriter(urgOut);
                        pw.println(sql);
                        throw new RuntimeException("sql execute error :" + sql, e);
                    }
                }
            }
            try {
                session.execute(sql.toString());
            } catch (SQLException e) {
                FileOutputStream urgOut = null;
                try {
                    urgOut = new FileOutputStream("XXX");
                } catch (FileNotFoundException fileNotFoundException) {
                    throw new RuntimeException(fileNotFoundException);
                }
                PrintWriter pw = new PrintWriter(urgOut);
                pw.println(sql);
                throw new RuntimeException("sql execute error :" + sql, e);
            }
        }
        try {
            session.submit();
        } catch (
                SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Multimap<String, Line> group(Collection<Line> lines) {
        HashMultimap<String, Line> multimap = HashMultimap.create();
        for (Line line : lines) {
            multimap.put(line.getTableName(), line);
        }
        return multimap;
    }

    private static void querySchema(MysqlSessions session, RMGraph graph) throws SQLException {
        String sql = "SELECT * FROM SESSION";
        ResultSet resultSet = session.select(sql);
        Map<String, Schema> schemas = new HashMap<>();
        while (resultSet.next()) {
            String tableName = resultSet.getString(Schema.TABLE_NAME);
            Schema schema = new Schema(tableName);
            int hasId = resultSet.getInt(Schema.HAS_ID);
            schema.setHasId(hasId);
            int type = resultSet.getInt(Schema.TYPE);
            String columnsJSON = resultSet.getString(Schema.COLUMNS);
            String in = resultSet.getString(Schema.IN);
            if (!resultSet.wasNull()) {
                schema.setIn(in);
            }
            String out = resultSet.getString(Schema.OUT);
            if (!resultSet.wasNull()) {
                schema.setOut(out);
            }
            String inLabel = resultSet.getString(Schema.IN_LABEL);
            if (!resultSet.wasNull()) {
                schema.setInLabel(inLabel);
            } else {
                schema.setInLabel(schema.getIn());
            }
            String outLabel = resultSet.getString(Schema.OUT_LABEL);
            if (!resultSet.wasNull()) {
                schema.setOutLabel(outLabel);
            } else {
                schema.setOutLabel(schema.getOut());
            }
            schema.setType(type);
            TypeReference<Map<String, String>> mapTypeReference = new TypeReference<Map<String, String>>() {
            };
            schema.setColumns(JSONUtil.toBean(columnsJSON, mapTypeReference, true));
            schemas.put(tableName, schema);
        }
        graph.setSchemas(schemas);
    }

    public static CountDownLatch countDownLatch;

    public static void query(MysqlSessions session, RMGraph graph) throws SQLException {
        querySchema(session, graph);
        countDownLatch = new CountDownLatch(graph.getSchemas().entrySet().size());
        for (Map.Entry<String, Schema> entry : graph.getSchemas().entrySet()) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT * FROM `");
            sql.append(entry.getKey()).append("`");
//            sql.append("Limit 5000");
            String select = sql.toString();
            ArrayList<Line> lines = new ArrayList<>();
            ResultSet resultSet = session.select(select, lines);
            Schema schema = entry.getValue();
            while (resultSet.next()) {
                Line line = new Line();
                line.setTableName(entry.getKey());
                String id = null;
                if (schema.getHasId() == 0) {
                    id = resultSet.getString(Schema.KEY);
                }
                if (StringUtils.isBlank(id)) {
                    id = schema.getTableName() + ":" + UnifiedGraph.getNextID();
                } else if (id.charAt(0) <= '9' && id.charAt(0) >= '0') {
                    id = schema.getTableName() + ":" + id;
                }
                line.setId(id);
                Map<String, Object> values = new HashMap<>();
                if (!schema.isNode()) {
                    String in = resultSet.getString(schema.getIn());
                    values.put(schema.getIn(), in);
                    String out = resultSet.getString(schema.getOut());
                    values.put(schema.getOut(), out);
                }
                for (Map.Entry<String, String> columns : schema.getColumns().entrySet()) {
                    Object value = null;
                    switch (columns.getValue()) {
                        case Schema.BIGINT:
                            value = resultSet.getLong(columns.getKey());
                            break;
                        case Schema.INT:
                            value = resultSet.getInt(columns.getKey());
                            break;
                        case Schema.NUMERIC:
                            value = resultSet.getDouble(columns.getKey());
                            break;
                        case Schema.HUGE_TEXT:
                        case Schema.MID_LARGE_TEXT:
                        case Schema.LARGE_TEXT:
                        case Schema.MID_TEXT:
                        case Schema.SMALL_TEXT:
                        case Schema.VERY_HUGE_TEXT:
                            value = resultSet.getString(columns.getKey());
                            break;
                        case Schema.DATE:
                            value = resultSet.getDate(columns.getKey());
                    }
                    boolean b = resultSet.wasNull();
                    if (!b) {
                        values.put(columns.getKey(), value);
                    }
                }
                line.setValues(values);
                graph.getLines().put(id, line);
                lines.add(line);
            }
            List<Line> cache = lines;
            new Thread(() -> {
                try {
                    System.out.println(Thread.currentThread().getName() + " " + schema.getTableName() + " " + (schema.isNode() ? "node" : "edge"));
                    cache.forEach(asyncRM2UMG::addLine);
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
    }

    public static void waitAll() throws InterruptedException {
        countDownLatch.await();
    }
}
