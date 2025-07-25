package io.github.intisy.utils.database;

import io.github.intisy.utils.log.Log;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings({"unused", "SqlNoDataSourceInspection", "SqlSourceToSinkFlow"})
public class SQL {
    private static final Pattern VALID_IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
    private static final int MAX_IDENTIFIER_LENGTH = 64;

    private final String url;
    private final String username;
    private final String password;
    private final DatabaseType databaseType;
    private Connection connection;
    private boolean autoCommit = true;

    public enum DatabaseType {
        SQLITE,
        MYSQL,
        UNKNOWN
    }

    public SQL(File dbFile) {
        this("jdbc:sqlite:" + requireNonNull(dbFile.getAbsolutePath(), "dbFilePath cannot be null"),
                null, null);
    }

    public SQL(String host, int port, String database, String username, String password) {
        this(String.format("jdbc:mysql://%s:%d/%s",
                        requireNonNull(host, "host cannot be null"),
                        port,
                        requireNonNull(database, "database cannot be null")),
                requireNonNull(username, "username cannot be null"),
                requireNonNull(password, "password cannot be null"));
    }

    public SQL(String jdbcUrl, String username, String password) {
        this.url = requireNonNull(jdbcUrl, "jdbcUrl cannot be null");
        this.username = username;
        this.password = password;
        this.databaseType = detectDatabaseType(this.url);
        this.connection = initializeConnection();
        Log.debug("SQL connection initialized for " + this.url + " (Type: " + this.databaseType + ")");
    }

    private static <T> T requireNonNull(T obj, String message) {
        if (obj == null) {
            throw new NullPointerException(message);
        }
        return obj;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    private static DatabaseType detectDatabaseType(String jdbcUrl) {
        String lowerUrl = jdbcUrl.toLowerCase();
        if (lowerUrl.startsWith("jdbc:sqlite:")) {
            return DatabaseType.SQLITE;
        } else if (lowerUrl.startsWith("jdbc:mysql:")) {
            return DatabaseType.MYSQL;
        }
        return DatabaseType.UNKNOWN;
    }

    private Connection initializeConnection() {
        try {
            Connection conn;
            if (username != null && password != null) {
                conn = DriverManager.getConnection(url, username, password);
            } else {
                conn = DriverManager.getConnection(url);
            }

            conn.setAutoCommit(this.autoCommit);

            if (databaseType == DatabaseType.SQLITE) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute("PRAGMA foreign_keys = ON");
                    Log.debug("SQLite PRAGMA foreign_keys=ON set.");
                }
            }

            return conn;
        } catch (SQLException e) {
            Log.error("Failed to initialize database connection (" + url + "): " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Log.warn("Connection was closed or null. Attempting to reconnect...");
            connection = initializeConnection();
        }
        return connection;
    }

    public void beginTransaction() throws SQLException {
        Connection conn = getConnection();
        if (autoCommit) {
            conn.setAutoCommit(false);
            autoCommit = false;
            Log.debug("Transaction started.");
        } else {
            Log.warn("beginTransaction called while already in a transaction.");
        }
    }

    public void commitTransaction() throws SQLException {
        Connection conn = getConnection();
        if (!autoCommit) {
            try {
                conn.commit();
                Log.debug("Transaction committed.");
            } catch (SQLException e) {
                Log.error("Transaction commit failed: " + e.getMessage());
                throw new RuntimeException(e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                    autoCommit = true;
                } catch (SQLException e) {
                    Log.error("Failed to reset autoCommit after commit: " + e.getMessage());
                }
            }
        } else {
            Log.warn("commitTransaction called without an active transaction.");
        }
    }

    public void rollbackTransaction() throws SQLException {
        Connection conn = getConnection();
        if (!autoCommit) {
            try {
                conn.rollback();
                Log.warn("Transaction rolled back.");
            } catch (SQLException e) {
                Log.error("Transaction rollback failed: " + e.getMessage());
                throw new RuntimeException(e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                    autoCommit = true;
                } catch (SQLException e) {
                    Log.error("Failed to reset autoCommit after rollback: " + e.getMessage());
                }
            }
        } else {
            Log.warn("rollbackTransaction called without an active transaction.");
        }
    }

    public void close() {
        if (connection != null) {
            try {
                if (!autoCommit) {
                    Log.warn("Closing connection with an uncommitted transaction. Rolling back.");
                    try {
                        connection.rollback();
                    } catch (SQLException ex) {
                        Log.error("Error rolling back transaction during close: " + ex.getMessage());
                    } finally {
                        try {
                            if (!connection.isClosed()) {
                                connection.setAutoCommit(true);
                            }
                        } catch (SQLException ignored) {}
                    }
                }
                if (!connection.isClosed()) {
                    connection.close();
                    Log.debug("Database connection closed.");
                }
            } catch (SQLException e) {
                Log.error("Error closing database connection: " + e.getMessage());
            } finally {
                connection = null;
            }
        }
    }

    private void validateIdentifier(String identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier (table/column name) cannot be null.");
        }
        if (identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier cannot be empty.");
        }
        if (identifier.length() > MAX_IDENTIFIER_LENGTH) {
            throw new IllegalArgumentException("Identifier '" + identifier + "' exceeds maximum length of " + MAX_IDENTIFIER_LENGTH);
        }
        if (!VALID_IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid identifier: '" + identifier + "'. Must match pattern: " + VALID_IDENTIFIER_PATTERN.pattern());
        }
    }

    public void createTable(String tableName, List<String> columnDefs) {
        createTable(tableName, columnDefs, null);
    }

    public void createTable(String tableName, List<String> columnDefs, List<String> constraints) {
        validateIdentifier(tableName);
        if (columnDefs == null || columnDefs.isEmpty()) {
            throw new IllegalArgumentException("At least one column definition is required.");
        }
        for(String colDef : columnDefs) {
            if(colDef == null || colDef.trim().isEmpty()) {
                throw new IllegalArgumentException("Column definition cannot be null or empty.");
            }
        }

        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(quoteIdentifier(tableName)).append(" (");

        sql.append(String.join(", ", columnDefs));

        if (constraints != null) {
            for(String constraint : constraints) {
                if(constraint != null && !constraint.trim().isEmpty()) {
                    sql.append(", ").append(constraint);
                } else {
                    throw new IllegalArgumentException("Table constraint cannot be null or empty.");
                }
            }
        }
        sql.append(")");

        String sqlString = sql.toString();
        Log.debug("Executing DDL: " + sqlString);

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sqlString);
            Log.info("Table '" + tableName + "' created or already exists.");
        } catch (SQLException e) {
            Log.error("Failed to create table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
    }

    public void deleteTable(String tableName) {
        validateIdentifier(tableName);
        String sql = "DROP TABLE IF EXISTS " + quoteIdentifier(tableName);
        Log.warn("Executing DDL (DROP TABLE): " + sql);

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Table '" + tableName + "' dropped or did not exist.");
        } catch (SQLException e) {
            Log.error("Failed to drop table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    public List<Map<String, Object>> executeRawQuery(String sql) {
        Log.warn("Executing raw query: " + sql);
        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String colName = metaData.getColumnLabel(i);
                    if (colName == null || colName.isEmpty()) {
                        colName = metaData.getColumnName(i);
                    }
                    row.put(colName, rs.getObject(i));
                }
                results.add(row);
            }

            Log.debug("Query returned " + results.size() + " rows");
            return results;
        } catch (SQLException e) {
            Log.error("Query failed: " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> executeQuery(String sql, List<?> params) {
        Log.debug("Executing parameterized query: " + sql);
        List<Map<String, Object>> results = new ArrayList<>();

        try (PreparedStatement pstmt = getConnection().prepareStatement(sql)) {
            bindParameters(pstmt, params);

            boolean isQuery = sql.trim().toLowerCase().startsWith("select");

            if (isQuery) {
                try (ResultSet rs = pstmt.executeQuery()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            String colName = metaData.getColumnLabel(i);
                            if (colName == null || colName.isEmpty()) {
                                colName = metaData.getColumnName(i);
                            }
                            row.put(colName, rs.getObject(i));
                        }
                        results.add(row);
                    }
                }
                Log.debug("Query returned " + results.size() + " rows");
            } else {
                int affected = pstmt.executeUpdate();
                Log.debug("Update affected " + affected + " rows");
                Map<String, Object> resultRow = new LinkedHashMap<>();
                resultRow.put("affectedRows", affected);
                results.add(resultRow);
            }

            return results;
        } catch (SQLException e) {
            Log.error("Query failed: " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> executeQuery(String sql, Object... params) {
        return executeQuery(sql, Arrays.asList(params));
    }

    public int executeUpdate(String sql) {
        Log.warn("Executing update: " + sql);
        try (Statement stmt = getConnection().createStatement()) {
            int affected = stmt.executeUpdate(sql);
            Log.debug("Update affected " + affected + " rows");
            return affected;
        } catch (SQLException e) {
            Log.error("Update failed: " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public int executeUpdate(String sql, List<?> params) {
        Log.warn("Executing parameterized update: " + sql);
        try (PreparedStatement pstmt = getConnection().prepareStatement(sql)) {
            bindParameters(pstmt, params);
            int affected = pstmt.executeUpdate();
            Log.debug("Update affected " + affected + " rows");
            return affected;
        } catch (SQLException e) {
            Log.error("Update failed: " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public int executeUpdate(String sql, Object... params) {
        return executeUpdate(sql, Arrays.asList(params));
    }

    public List<Map<String, Object>> selectData(String tableName, List<String> columns, Map<String, Object> whereClause, String orderBy) {
        return selectData(tableName, columns, whereClause, orderBy, 0, 0);
    }

    public List<Map<String, Object>> selectData(String tableName, List<String> columns, Map<String, Object> whereClause, String orderBy, int limit) {
        return selectData(tableName, columns, whereClause, orderBy, limit, 0);
    }

    public List<Map<String, Object>> selectData(String tableName, List<String> columns, Map<String, Object> whereClause, String orderBy, int limit, int offset) {
        validateIdentifier(tableName);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be specified.");
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        if (columns.size() == 1 && "*".equals(columns.get(0))) {
            sql.append("*");
        } else {
            columns.forEach(this::validateIdentifier);
            sql.append(columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")));
        }
        sql.append(" FROM ").append(quoteIdentifier(tableName));

        List<Object> queryParams = new ArrayList<>();
        if (whereClause != null && !whereClause.isEmpty()) {
            String where = whereClause.entrySet().stream()
                .map(entry -> {
                    validateIdentifier(entry.getKey());
                    return quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS NULL" : " = ?");
                })
                .collect(Collectors.joining(" AND "));
            sql.append(" WHERE ").append(where);
            queryParams.addAll(whereClause.values().stream().filter(Objects::nonNull).collect(Collectors.toList()));
        }

        if (orderBy != null && !orderBy.trim().isEmpty()) {
            String[] parts = orderBy.split(",");
            List<String> orderByClauses = new ArrayList<>();
            for (String part : parts) {
                String[] colOrder = part.trim().split("\\s+");
                validateIdentifier(colOrder[0]);
                String clause = quoteIdentifier(colOrder[0]);
                if (colOrder.length > 1 && ("ASC".equalsIgnoreCase(colOrder[1]) || "DESC".equalsIgnoreCase(colOrder[1]))) {
                    clause += " " + colOrder[1].toUpperCase();
                }
                orderByClauses.add(clause);
            }
            sql.append(" ORDER BY ").append(String.join(", ", orderByClauses));
        }

        if (limit > 0) {
            sql.append(" LIMIT ?");
            queryParams.add(limit);
            if (offset > 0) {
                sql.append(" OFFSET ?");
                queryParams.add(offset);
            }
        }

        return executeQuery(sql.toString(), queryParams);
    }

    public int upsertData(String tableName, List<String> conflictColumns, Map<String, Object> insertData) {
        validateIdentifier(tableName);
        if (conflictColumns == null || conflictColumns.isEmpty()) {
            throw new IllegalArgumentException("Conflict columns list cannot be null or empty.");
        }
        if (insertData == null || insertData.isEmpty()) {
            throw new IllegalArgumentException("Insert data map cannot be null or empty.");
        }

        for (String col : conflictColumns) {
            validateIdentifier(col);
        }
        List<String> insertColumns = new ArrayList<>();
        List<Object> insertValues = new ArrayList<>();
        for (Map.Entry<String, Object> entry : new LinkedHashMap<>(insertData).entrySet()) {
            validateIdentifier(entry.getKey());
            insertColumns.add(quoteIdentifier(entry.getKey()));
            insertValues.add(entry.getValue());
        }

        String sql;
        List<String> updateAssignments = new ArrayList<>();

        switch (databaseType) {
            case SQLITE:
                for (String col : insertColumns) {
                    String unquotedCol = col;
                    if (col.startsWith("\"") && col.endsWith("\"")) {
                        unquotedCol = col.substring(1, col.length() - 1);
                    }
                    if (!conflictColumns.contains(unquotedCol)) {
                        updateAssignments.add(col + " = excluded." + col);
                    }
                }
                if (updateAssignments.isEmpty()) {
                    if (insertColumns.size() == conflictColumns.size()) {
                        throw new IllegalArgumentException("Upsert requires at least one column to update that is not part of the conflict key.");
                    } else {
                        throw new IllegalStateException("Failed to generate update assignments for ON CONFLICT clause.");
                    }
                }

                sql = String.format(
                        "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                        quoteIdentifier(tableName),
                        String.join(", ", insertColumns),
                        String.join(", ", Collections.nCopies(insertColumns.size(), "?")),
                        String.join(", ", conflictColumns.stream().map(this::quoteIdentifier).toArray(String[]::new)),
                        String.join(", ", updateAssignments)
                );
                break;

            case MYSQL:
                for (String col : insertColumns) {
                    String unquotedCol = col;
                    if (col.startsWith("`") && col.endsWith("`")) {
                        unquotedCol = col.substring(1, col.length() - 1);
                    }
                    if (!conflictColumns.contains(unquotedCol)) {
                        updateAssignments.add(col + " = VALUES(" + col + ")");
                    }
                }
                if (updateAssignments.isEmpty()) {
                    if (insertColumns.size() == conflictColumns.size()) {
                        throw new IllegalArgumentException("Upsert requires at least one column to update that is not part of the primary/unique key.");
                    } else {
                        throw new IllegalStateException("Failed to generate update assignments for ON DUPLICATE KEY UPDATE clause.");
                    }
                }

                sql = String.format(
                        "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                        quoteIdentifier(tableName),
                        String.join(", ", insertColumns),
                        String.join(", ", Collections.nCopies(insertColumns.size(), "?")),
                        String.join(", ", updateAssignments)
                );
                break;

            default:
                Log.error("Upsert operation is not supported for database type: " + databaseType);
                throw new UnsupportedOperationException("Upsert not supported for database type: " + databaseType);
        }

        Log.debug("Executing upsert: " + sql + " with " + insertValues.size() + " parameters.");

        try (PreparedStatement pstmt = getConnection().prepareStatement(sql)) {
            bindParameters(pstmt, insertValues);
            int affectedRows = pstmt.executeUpdate();
            Log.debug("Upsert successful, " + affectedRows + " row(s) affected.");
            return affectedRows;
        } catch (SQLException e) {
            Log.error("Upsert failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public int insertData(String tableName, Map<String, Object> insertData) {
        validateIdentifier(tableName);
        if (insertData == null || insertData.isEmpty()) {
            throw new IllegalArgumentException("Insert data map cannot be null or empty.");
        }

        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : new LinkedHashMap<>(insertData).entrySet()) {
            validateIdentifier(entry.getKey());
            columns.add(quoteIdentifier(entry.getKey()));
            values.add(entry.getValue());
        }

        String sql = buildInsertStatement(tableName, columns);
        Log.debug("Executing insert: " + sql + " with " + values.size() + " values.");

        try (PreparedStatement pstmt = getConnection().prepareStatement(sql)) {
            bindParameters(pstmt, values);
            int affectedRows = pstmt.executeUpdate();
            Log.debug("Insert successful, " + affectedRows + " row(s) affected.");
            return affectedRows;
        } catch (SQLException e) {
            Log.error("Insert failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public int insertData(String tableName, Object... columnsAndValues) {
        if (columnsAndValues.length == 0) {
            throw new IllegalArgumentException("No column/value pairs provided for insert.");
        }
        if (columnsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Columns and values must be provided in pairs.");
        }

        Map<String, Object> insertData = new LinkedHashMap<>();
        for (int i = 0; i < columnsAndValues.length; i += 2) {
            if (!(columnsAndValues[i] instanceof String)) {
                throw new IllegalArgumentException("Column name at index " + i + " must be a String.");
            }
            Object value = columnsAndValues[i + 1];
            insertData.put((String) columnsAndValues[i], value);
        }

        return insertData(tableName, insertData);
    }

    public int insertData(String tableName, LinkedHashMap<String, Object> insertData) {
        return insertData(tableName, (Map<String, Object>) insertData);
    }

    public int insertDataIfEmpty(String tableName, Object... columnsAndValues) {
        validateIdentifier(tableName);
        if (columnsAndValues.length < 2) {
            throw new IllegalArgumentException("Must provide at least one column-value pair for checking existence.");
        }
        if (!(columnsAndValues[0] instanceof String)) {
            throw new IllegalArgumentException("First element (check column) must be a String.");
        }
        String checkColumn = (String) columnsAndValues[0];
        Object checkValue = columnsAndValues[1];
        validateIdentifier(checkColumn);

        Map<String, Object> where = new HashMap<>();
        where.put(checkColumn, checkValue);
        List<Map<String, Object>> existing = selectData(tableName, Collections.singletonList(checkColumn), where);

        if (existing.isEmpty()) {
            Log.debug("No existing record found for " + checkColumn + ". Inserting...");
            return insertData(tableName, columnsAndValues);
        } else {
            Log.debug("Skip insert - data already exists for " + checkColumn);
            return 0;
        }
    }

    public int updateData(String tableName, Map<String, Object> setClause, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (setClause == null || setClause.isEmpty()) {
            throw new IllegalArgumentException("SET clause map cannot be null or empty.");
        }
        if (whereClause == null) {
            whereClause = Collections.emptyMap();
            Log.warn("updateData called with null whereClause - will update all rows in table '" + tableName + "'!");
        }

        List<String> setAssignments = new ArrayList<>();
        List<Object> setValues = new ArrayList<>();
        for (Map.Entry<String, Object> entry : setClause.entrySet()) {
            validateIdentifier(entry.getKey());
            setAssignments.add(quoteIdentifier(entry.getKey()) + " = ?");
            setValues.add(entry.getValue());
        }

        List<String> whereConditions = new ArrayList<>();
        List<Object> whereValues = new ArrayList<>();
        for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
            validateIdentifier(entry.getKey());
            whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS NULL" : " = ?"));
            if(entry.getValue() != null) {
                whereValues.add(entry.getValue());
            }
        }

        StringBuilder sql = new StringBuilder("UPDATE ")
                .append(quoteIdentifier(tableName))
                .append(" SET ")
                .append(String.join(", ", setAssignments));

        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        String sqlString = sql.toString();
        List<Object> allParams = new ArrayList<>(setValues);
        allParams.addAll(whereValues);

        Log.debug("Executing update: " + sqlString + " with " + allParams.size() + " parameters.");

        try (PreparedStatement pstmt = getConnection().prepareStatement(sqlString)) {
            bindParameters(pstmt, allParams);
            int affectedRows = pstmt.executeUpdate();
            Log.debug("Update successful, " + affectedRows + " row(s) affected.");
            return affectedRows;
        } catch (SQLException e) {
            Log.error("Update failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
    }

    public int deleteData(String tableName, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (whereClause == null) {
            whereClause = Collections.emptyMap();
            Log.warn("deleteData called with null whereClause - will delete all rows from table '" + tableName + "'!");
        }

        List<String> whereConditions = new ArrayList<>();
        List<Object> whereValues = new ArrayList<>();
        for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
            validateIdentifier(entry.getKey());
            whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS NULL" : " = ?"));
            if(entry.getValue() != null) {
                whereValues.add(entry.getValue());
            }
        }

        StringBuilder sql = new StringBuilder("DELETE FROM ")
                .append(quoteIdentifier(tableName));

        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        String sqlString = sql.toString();
        Log.debug("Executing delete: " + sqlString + " with " + whereValues.size() + " parameters.");

        try (PreparedStatement pstmt = getConnection().prepareStatement(sqlString)) {
            bindParameters(pstmt, whereValues);
            int affectedRows = pstmt.executeUpdate();
            Log.debug("Delete successful, " + affectedRows + " row(s) affected.");
            return affectedRows;
        } catch (SQLException e) {
            Log.error("Delete failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> selectData(String tableName, List<String> columnsToSelect, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (columnsToSelect == null || columnsToSelect.isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one column to select (or '*').");
        }

        String selectColsString;
        if (columnsToSelect.size() == 1 && "*".equals(columnsToSelect.get(0))) {
            selectColsString = "*";
        } else {
            List<String> quotedCols = new ArrayList<>();
            for(String col : columnsToSelect) {
                validateIdentifier(col);
                quotedCols.add(quoteIdentifier(col));
            }
            selectColsString = String.join(", ", quotedCols);
        }

        if (whereClause == null) {
            whereClause = Collections.emptyMap();
        }

        List<String> whereConditions = new ArrayList<>();
        List<Object> whereValues = new ArrayList<>();
        for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
            validateIdentifier(entry.getKey());
            whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS NULL" : " = ?"));
            if(entry.getValue() != null) {
                whereValues.add(entry.getValue());
            }
        }

        StringBuilder sql = new StringBuilder("SELECT ")
                .append(selectColsString)
                .append(" FROM ")
                .append(quoteIdentifier(tableName));

        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        String sqlString = sql.toString();
        Log.debug("Executing select: " + sqlString + " with " + whereValues.size() + " parameters.");

        List<Map<String, Object>> results = new ArrayList<>();
        try (PreparedStatement pstmt = getConnection().prepareStatement(sqlString)) {
            bindParameters(pstmt, whereValues);
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = metaData.getColumnLabel(i);
                        if (colName == null || colName.isEmpty()) {
                            colName = metaData.getColumnName(i);
                        }
                        row.put(colName, rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        } catch (SQLException e) {
            Log.error("Select failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
        Log.debug("Select executed, " + results.size() + " row(s) returned.");
        return results;
    }

    public List<Map<String, Object>> selectData(String tableName, List<String> columnsToSelect, String whereColumn, Object whereValue) {
        Map<String, Object> whereClause = new LinkedHashMap<>();
        if (whereColumn != null) {
            validateIdentifier(whereColumn);
            whereClause.put(whereColumn, whereValue);
        }
        return selectData(tableName, columnsToSelect, whereClause);
    }

    public <T> List<T> selectSingleColumn(String tableName, String columnToSelect, Map<String, Object> whereClause, Class<T> expectedType) throws SQLException {
        validateIdentifier(columnToSelect);

        List<Map<String, Object>> rawResults = selectData(tableName, Collections.singletonList(columnToSelect), whereClause);
        List<T> results = new ArrayList<>();

        for(Map<String, Object> row : rawResults) {
            Object value = row.get(columnToSelect);
            if(value == null) {
                results.add(null);
            } else if (expectedType.isInstance(value)) {
                results.add(expectedType.cast(value));
            } else {
                try {
                    if (expectedType == String.class) {
                        results.add(expectedType.cast(value.toString()));
                    } else if (expectedType == Integer.class && value instanceof Number) {
                        results.add(expectedType.cast(((Number) value).intValue()));
                    } else if (expectedType == Long.class && value instanceof Number) {
                        results.add(expectedType.cast(((Number) value).longValue()));
                    } else if (expectedType == Double.class && value instanceof Number) {
                        results.add(expectedType.cast(((Number) value).doubleValue()));
                    } else if (expectedType == Float.class && value instanceof Number) {
                        results.add(expectedType.cast(((Number) value).floatValue()));
                    } else if (expectedType == Boolean.class) {
                        if(value instanceof Boolean) {
                            results.add(expectedType.cast(value));
                        } else if(value instanceof Number) {
                            results.add(expectedType.cast(((Number)value).intValue() != 0));
                        } else if (value instanceof String) {
                            results.add(expectedType.cast(Boolean.parseBoolean((String)value)));
                        } else {
                            throw new ClassCastException("Cannot reliably cast " + value.getClass().getName() + " to Boolean");
                        }
                    }
                    else {
                        throw new ClassCastException("Cannot automatically cast value of type " + value.getClass().getName() + " to " + expectedType.getName());
                    }
                } catch (ClassCastException e) {
                    Log.error("Type mismatch for column '" + columnToSelect + "'. Expected " + expectedType.getName() + ", but got " + value.getClass().getName() + ". Value: " + value);
                    throw new SQLException("Type mismatch retrieving column " + columnToSelect + ": " + e.getMessage(), e);
                }
            }
        }
        return results;
    }

    public int[] insertBatchData(String tableName, List<Map<String, Object>> dataRows) throws SQLException {
        validateIdentifier(tableName);
        if (dataRows == null || dataRows.isEmpty()) {
            throw new IllegalArgumentException("Data rows list cannot be null or empty for batch insert.");
        }

        Map<String, Object> firstRow = dataRows.get(0);
        if (firstRow == null || firstRow.isEmpty()) {
            throw new IllegalArgumentException("First data row map cannot be null or empty.");
        }
        Set<String> columnSet = new LinkedHashSet<>(firstRow.keySet());
        if(columnSet.isEmpty()) {
            throw new IllegalArgumentException("No columns found in the first data row.");
        }
        List<String> columns = new ArrayList<>(columnSet);
        List<String> quotedColumns = new ArrayList<>();
        for(String col : columns) {
            validateIdentifier(col);
            quotedColumns.add(quoteIdentifier(col));
        }

        String sql = buildInsertStatement(tableName, quotedColumns);
        Log.debug("Preparing batch insert: " + sql);

        Connection conn = getConnection();
        boolean startedTransaction = false;
        int[] batchResults = {};

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            if (conn.getAutoCommit()) {
                conn.setAutoCommit(false);
                startedTransaction = true;
                Log.debug("Temporarily disabled autoCommit for batch insert.");
            }

            int batchCount = 0;
            for (Map<String, Object> row : dataRows) {
                if (row == null || row.size() != columns.size()) {
                    throw new IllegalArgumentException("Inconsistent row data size in batch. Expected " + columns.size() + " columns based on first row. Found: " + (row == null ? "null" : row.size()));
                }
                List<Object> values = new ArrayList<>();
                for (String col : columns) {
                    if (!row.containsKey(col)) {
                        throw new IllegalArgumentException("Row is missing column '" + col + "' required for batch insert (based on first row keys).");
                    }
                    values.add(row.get(col));
                }
                bindParameters(pstmt, values);
                pstmt.addBatch();
                batchCount++;
            }

            if (batchCount > 0) {
                Log.debug("Executing batch insert with " + batchCount + " statements.");
                batchResults = pstmt.executeBatch();
                Log.debug("Batch insert executed.");
            }

            if (startedTransaction) {
                conn.commit();
                Log.debug("Committed batch insert transaction.");
            }

        } catch (SQLException | IllegalArgumentException e) {
            Log.error("Batch insert failed for table '" + tableName + "': " + e.getMessage());
            if (startedTransaction) {
                try {
                    conn.rollback();
                    Log.warn("Rolled back batch insert transaction due to error.");
                } catch (SQLException rbEx) {
                    Log.error("Failed to rollback batch insert transaction: " + rbEx.getMessage());
                }
            }
            if (e instanceof SQLException) throw (SQLException) e;
            else throw new SQLException("Batch insert failed: " + e.getMessage(), e);
        } finally {
            if (startedTransaction) {
                try {
                    if (!conn.isClosed()) {
                        conn.setAutoCommit(true);
                        Log.debug("Restored autoCommit state.");
                    }
                } catch (SQLException acEx) {
                    Log.error("Failed to restore autoCommit state after batch insert: " + acEx.getMessage());
                }
            }
        }
        return batchResults;
    }

    private String quoteIdentifier(String identifier) {
        return identifier;
    }

    private String buildInsertStatement(String tableName, List<String> columns) {
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ", Collections.nCopies(columns.size(), "?"));
        return "INSERT INTO " + quoteIdentifier(tableName) + " (" + cols + ") VALUES (" + placeholders + ")";
    }

    private void bindParameters(PreparedStatement pstmt, List<?> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object param = params.get(i);
            if (param == null) {
                pstmt.setNull(i + 1, Types.VARCHAR);
            } else {
                pstmt.setObject(i + 1, param);
            }
        }
    }

    public void logDatabase() {
        Log.info("--- Logging Database Schema and Content (" + url + ") ---");
        try {
            DatabaseMetaData metaData = getConnection().getMetaData();
            String catalog = getConnection().getCatalog();
            String schemaPattern = (databaseType == DatabaseType.MYSQL) ? catalog : null;

            try (ResultSet tables = metaData.getTables(catalog, schemaPattern, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    logTable(tableName, metaData);
                }
            }
        } catch (SQLException e) {
            Log.error("Failed to list tables: " + e.getMessage());
            throw new RuntimeException(e);
        }
        Log.info("--- Finished Logging Database ---");
    }

    private void logTable(String tableName, DatabaseMetaData metaData) throws SQLException {
        List<String> columns = getTableColumns(tableName, metaData);
        if (columns.isEmpty()) {
            Log.info("Table: " + tableName + " (No columns found or table does not exist)");
            return;
        }
        logTableContents(tableName, columns);
    }

    private List<String> getTableColumns(String tableName, DatabaseMetaData metaData) throws SQLException {
        List<String> columns = new ArrayList<>();
        String catalog = getConnection().getCatalog();
        String schemaPattern = (databaseType == DatabaseType.MYSQL) ? catalog : null;

        try (ResultSet cols = metaData.getColumns(catalog, schemaPattern, tableName, null)) {
            while (cols.next()) {
                columns.add(cols.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            Log.error("Failed to get columns for table '" + tableName + "': " + e.getMessage());
            throw new RuntimeException(e);
        }
        return columns;
    }

    private void logTableContents(String tableName, List<String> columns) {
        List<List<String>> rows = new ArrayList<>();
        int[] columnWidths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnWidths[i] = columns.get(i).length();
        }

        String sql = "SELECT * FROM " + quoteIdentifier(tableName);
        try (PreparedStatement pstmt = getConnection().prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 0; i < columns.size(); i++) {
                    String value = rs.getString(i + 1);
                    value = (value == null ? "NULL" : value);
                    row.add(value);
                    columnWidths[i] = Math.max(columnWidths[i], value.length());
                }
                rows.add(row);
            }
        } catch (SQLException e) {
            Log.error("Failed to retrieve contents for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }

        StringBuilder headerBuilder = new StringBuilder("|");
        StringBuilder dividerBuilder = new StringBuilder("+");
        for (int i = 0; i < columns.size(); i++) {
            String headerCell = " " + padRight(columns.get(i), columnWidths[i]) + " ";
            headerBuilder.append(headerCell).append("|");
            dividerBuilder.append(String.join("", Collections.nCopies(columnWidths[i] + 2, "-"))).append("+");
        }

        String title = " Table: " + tableName + " (" + rows.size() + " rows) ";
        int totalWidth = dividerBuilder.length();
        int titlePaddingCount = (totalWidth - title.length()) / 2;
        String titlePadding = titlePaddingCount > 0 ? String.join("", Collections.nCopies(titlePaddingCount, "-")) : "";
        String endPadding = String.join("", Collections.nCopies(Math.max(0, totalWidth - title.length() - titlePaddingCount), "-"));
        String titleLine = titlePadding + title + endPadding;

        Log.info("");
        Log.info(titleLine);
        Log.info(headerBuilder.toString());
        Log.info(dividerBuilder.toString());

        if (rows.isEmpty()) {
            String emptyRow = "| " + padRight("(No data)", totalWidth - 4) + " |";
            Log.info(emptyRow);
        } else {
            for (List<String> row : rows) {
                StringBuilder rowBuilder = new StringBuilder("|");
                for (int i = 0; i < columns.size(); i++) {
                    rowBuilder.append(" ").append(padRight(row.get(i), columnWidths[i])).append(" |");
                }
                Log.info(rowBuilder.toString());
            }
        }
        Log.info(dividerBuilder.toString());
        Log.info("");
    }

    private static String padRight(String s, int n) {
        if (s.length() >= n) {
            return s;
        }
        StringBuilder sb = new StringBuilder(n);
        sb.append(s);
        for (int i = s.length(); i < n; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    public boolean tableExists(String tableName) {
        validateIdentifier(tableName);

        try {
            DatabaseMetaData metaData = getConnection().getMetaData();
            String catalog = getConnection().getCatalog();
            String schemaPattern = (databaseType == DatabaseType.MYSQL) ? catalog : null;

            try (ResultSet tables = metaData.getTables(
                    catalog,
                    schemaPattern,
                    tableName,
                    new String[]{"TABLE"})) {

                boolean exists = tables.next();
                Log.debug("Table '" + tableName + "' " + (exists ? "exists" : "does not exist"));
                return exists;
            }
        } catch (SQLException e) {
            Log.error("Failed to check if table '" + tableName + "' exists: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public long count(String tableName) {
        return count(tableName, null);
    }

    public long count(String tableName, Map<String, Object> whereClause) {
        validateIdentifier(tableName);

        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ")
                .append(quoteIdentifier(tableName));

        List<Object> whereValues = new ArrayList<>();
        if (whereClause != null && !whereClause.isEmpty()) {
            String where = whereClause.entrySet().stream()
                .map(entry -> {
                    validateIdentifier(entry.getKey());
                    return quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS NULL" : " = ?");
                })
                .collect(Collectors.joining(" AND "));
            sql.append(" WHERE ").append(where);
            whereValues.addAll(whereClause.values().stream().filter(Objects::nonNull).collect(Collectors.toList()));
        }

        String sqlString = sql.toString();
        Log.debug("Executing count: " + sqlString);

        try (PreparedStatement pstmt = getConnection().prepareStatement(sqlString)) {
            if (!whereValues.isEmpty()) {
                bindParameters(pstmt, whereValues);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    Log.debug("Count returned: " + count);
                    return count;
                }
                return 0;
            }
        } catch (SQLException e) {
            Log.error("Count failed for table '" + tableName + "': " +
                    e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql) {
        Log.warn("Executing SQL statement: " + sql);
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute(sql);
            Log.debug("Statement executed successfully");
        } catch (SQLException e) {
            Log.error("Statement execution failed: " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql, List<?> params) {
        Log.warn("Executing parameterized SQL statement: " + sql);
        try (PreparedStatement pstmt = getConnection().prepareStatement(sql)) {
            bindParameters(pstmt, params);
            pstmt.execute();
            Log.debug("Statement executed successfully");
        } catch (SQLException e) {
            Log.error("Statement execution failed: " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql, Object... params) {
        execute(sql, Arrays.asList(params));
    }

    public boolean updateTableSchema(String tableName, List<String> newColumnDefs) {
        return updateTableSchema(tableName, newColumnDefs, null);
    }

    public boolean updateTableSchema(String tableName, List<String> newColumnDefs, List<String> newConstraints) {
        validateIdentifier(tableName);
        if (newColumnDefs == null) {
            throw new IllegalArgumentException("Column definitions list cannot be null.");
        }

        try {
            List<String> actualColumnDefs = new ArrayList<>();
            List<String> allConstraints = (newConstraints == null) ? new ArrayList<>() : new ArrayList<>(newConstraints);
            Set<String> constraintKeywords = new HashSet<>(Arrays.asList("CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK"));

            for (String def : newColumnDefs) {
                if (def == null || def.trim().isEmpty()) {
                    throw new IllegalArgumentException("Column definition or constraint cannot be null or empty.");
                }
                String firstWord = def.trim().split("[\\s(]+")[0].toUpperCase();
                if (constraintKeywords.contains(firstWord)) {
                    allConstraints.add(def);
                } else {
                    actualColumnDefs.add(def);
                }
            }

            if (!tableExists(tableName)) {
                if (actualColumnDefs.isEmpty()) {
                    throw new IllegalArgumentException("At least one column definition is required for a new table.");
                }
                Log.warn("Table '" + tableName + "' does not exist. Creating it instead.");
                createTable(tableName, actualColumnDefs, allConstraints);
                return true;
            }

            Map<String, String> currentColumnsMap = getTableColumns(tableName, getConnection().getMetaData()).stream()
                    .collect(Collectors.toMap(String::toLowerCase, col -> col, (c1, c2) -> c1));

            Map<String, String> newColumnDefinitions = new LinkedHashMap<>();
            for (String colDef : actualColumnDefs) {
                String originalName = colDef.trim().split("\\s+")[0].replace("`", "").replace("\"", "");
                newColumnDefinitions.put(originalName.toLowerCase(), colDef);
            }

            Set<String> currentLower = currentColumnsMap.keySet();
            Set<String> newLower = newColumnDefinitions.keySet();

            Set<String> toRemove = new HashSet<>(currentLower);
            toRemove.removeAll(newLower);

            Set<String> toAdd = new HashSet<>(newLower);
            toAdd.removeAll(currentLower);

            boolean changesMade = false;

            if (databaseType == DatabaseType.SQLITE) {
                if (!toRemove.isEmpty() || !allConstraints.isEmpty()) {
                    recreateTableWithNewSchema(tableName, currentColumnsMap, newColumnDefinitions, allConstraints);
                    return true;
                }
                if (!toAdd.isEmpty()) {
                    for (String colLower : toAdd) {
                        String colDef = newColumnDefinitions.get(colLower);
                        execute("ALTER TABLE " + quoteIdentifier(tableName) + " ADD COLUMN " + colDef);
                        changesMade = true;
                    }
                }
            } else {
                if (!toAdd.isEmpty()) {
                    for (String colLower : toAdd) {
                        String colDef = newColumnDefinitions.get(colLower);
                        execute("ALTER TABLE " + quoteIdentifier(tableName) + " ADD COLUMN " + colDef);
                        changesMade = true;
                    }
                }
                if (!toRemove.isEmpty()) {
                    for (String colLower : toRemove) {
                        String originalColName = currentColumnsMap.get(colLower);
                        execute("ALTER TABLE " + quoteIdentifier(tableName) + " DROP COLUMN " + quoteIdentifier(originalColName));
                        changesMade = true;
                    }
                }
            }

            return changesMade;
        } catch (SQLException e) {
            Log.error("Failed to update table schema for '" + tableName + "': " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void recreateTableWithNewSchema(String tableName, Map<String, String> currentColumnsMap,
                                            Map<String, String> newColumnDefinitions, List<String> newConstraints)
            throws SQLException {

        Log.warn("Recreating table '" + tableName + "' to apply schema changes.");

        boolean wasAutoCommit = getConnection().getAutoCommit();
        if (wasAutoCommit) {
            getConnection().setAutoCommit(false);
        }

        try {
            String tempTableName = tableName + "_temp_" + System.currentTimeMillis();

            createTable(tempTableName, new ArrayList<>(newColumnDefinitions.values()), newConstraints);

            Set<String> currentLower = currentColumnsMap.keySet();
            Set<String> newLower = newColumnDefinitions.keySet();
            Set<String> commonLower = new HashSet<>(currentLower);
            commonLower.retainAll(newLower);

            if (!commonLower.isEmpty()) {
                String selectCols = commonLower.stream()
                        .map(currentColumnsMap::get)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

                String insertCols = commonLower.stream()
                        .map(newColumnDefinitions::get)
                        .map(def -> def.trim().split("\\s+")[0])
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));

                String copyDataSql = "INSERT INTO " + quoteIdentifier(tempTableName) + " (" + insertCols + ")" +
                        " SELECT " + selectCols + " FROM " + quoteIdentifier(tableName);

                Log.debug("Copying data to new table: " + copyDataSql);
                execute(copyDataSql);
            }

            deleteTable(tableName);

            execute("ALTER TABLE " + quoteIdentifier(tempTableName) + " RENAME TO " + quoteIdentifier(tableName));

            if (wasAutoCommit) {
                getConnection().commit();
            }
            Log.info("Successfully recreated table '" + tableName + "' with updated schema.");

        } catch (Exception e) {
            Log.error("Error during table recreation, attempting to rollback.");
            try {
                getConnection().rollback();
            } catch (SQLException rollbackEx) {
                Log.error("Failed to rollback transaction: " + rollbackEx.getMessage());
            }
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException("Failed to recreate table", e);
        } finally {
            if (wasAutoCommit) {
                try {
                    getConnection().setAutoCommit(true);
                } catch (SQLException autoCommitEx) {
                    Log.error("Failed to restore autoCommit: " + autoCommitEx.getMessage());
                }
            }
        }
    }
}