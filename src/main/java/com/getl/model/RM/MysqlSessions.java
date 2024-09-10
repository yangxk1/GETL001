/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getl.model.RM;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlSessions {

    private final String database;
    private final Connection conn;

    public MysqlSessions(String database, String username, String password) throws SQLException, ClassNotFoundException {
        this.database = database;
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection(database, username, password);
        conn.setAutoCommit(true);
    }

    public String database() {
        return this.database;
    }

    public ResultSet select(String sql, ArrayList<Line> lines) throws SQLException {
        System.out.println(sql);
        Statement statement = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        try {
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.last();//move to last line
            int count = resultSet.getRow();
            resultSet.beforeFirst();//move to start line
            if (lines != null) {
                lines.ensureCapacity(count);
            }
            System.out.println("RESULT COUNT: " + count);
            return resultSet;
        } catch (SQLException e) {
            statement.close();
            throw e;
        }
    }

    public ResultSet select(String sql) throws SQLException {
        return select(sql, null);
    }

    public void execute(String sql) throws SQLException {
        try (Statement statement = this.conn.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    public void submit() throws SQLException {
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
    }
}
