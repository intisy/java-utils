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
        conflictColumns.forEach(this::validateIdentifier);
        if (insertData == null || insertData.isEmpty()) {
            throw new IllegalArgumentException("Insert data cannot be null or empty.");
        }
        insertData.keySet().forEach(this::validateIdentifier);

        String sql;
        if (databaseType == DatabaseType.SQLITE) {
            sql = buildUpsertSqlite(tableName, conflictColumns, insertData);
        } else if (databaseType == DatabaseType.MYSQL) {
            sql = buildUpsertMysql(tableName, conflictColumns, insertData);
        } else {
            throw new UnsupportedOperationException("Upsert not supported for database type: " + databaseType);
        }

        List<Object> params = new ArrayList<>(insertData.values());
        if (databaseType == DatabaseType.MYSQL) {
            params.addAll(insertData.values()); // For ON DUPLICATE KEY UPDATE part
        }

        Log.debug("Executing upsert: " + sql);
        return executeUpdate(sql, params);
    }

    public int insertData(String tableName, Map<String, Object> data) {
        validateIdentifier(tableName);
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data map cannot be null or empty.");
        }
        data.keySet().forEach(this::validateIdentifier);

        String columns = data.keySet().stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = data.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", quoteIdentifier(tableName), columns, placeholders);

        Log.debug("Executing insert: " + sql);
        return executeUpdate(sql, new ArrayList<>(data.values()));
    }

    public int updateData(String tableName, Map<String, Object> data, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data map for update cannot be null or empty.");
        }
        data.keySet().forEach(this::validateIdentifier);

        List<String> setClauses = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            setClauses.add(quoteIdentifier(entry.getKey()) + " = ?");
            values.add(entry.getValue());
        }

        List<String> whereConditions = new ArrayList<>();
        List<Object> whereValues = new ArrayList<>();
        if (whereClause != null && !whereClause.isEmpty()) {
            for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
                validateIdentifier(entry.getKey());
                whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS NULL" : " = ?"));
                if(entry.getValue() != null) {
                    whereValues.add(entry.getValue());
                }
            }
        }

        StringBuilder sql = new StringBuilder("UPDATE ")
                .append(quoteIdentifier(tableName))
                .append(" SET ").append(String.join(", ", setClauses));

        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        values.addAll(whereValues);

        Log.debug("Executing update: " + sql);
        return executeUpdate(sql.toString(), values);
    }

    public int deleteData(String tableName, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (whereClause == null || whereClause.isEmpty()) {
            throw new IllegalArgumentException("Where clause map cannot be null or empty for delete operation to prevent accidental mass deletion.");
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
                .append(quoteIdentifier(tableName))
                .append(" WHERE ").append(String.join(" AND ", whereConditions));

        Log.debug("Executing delete: " + sql);
        return executeUpdate(sql.toString(), whereValues);
    }

    public List<Map<String, Object>> getData(String tableName, List<String> columns, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns list cannot be null or empty.");
        }
        columns.forEach(this::validateIdentifier);

        String selectedColumns = columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder("SELECT ")
                .append(selectedColumns)
                .append(" FROM ").append(quoteIdentifier(tableName));

        List<Object> whereValues = new ArrayList<>();
        if (whereClause != null && !whereClause.isEmpty()) {
            List<String> whereConditions = new ArrayList<>();
            for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
                validateIdentifier(entry.getKey());
                whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS NULL" : " = ?"));
                if(entry.getValue() != null) {
                    whereValues.add(entry.getValue());
                }
            }
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        Log.debug("Executing select: " + sql);
        return executeQuery(sql.toString(), whereValues);
    }

    public List<Map<String, Object>> getData(String tableName, List<String> columns) {
        return getData(tableName, columns, null);
    }

    public boolean tableExists(String tableName) {
        validateIdentifier(tableName);
        try {
            DatabaseMetaData dbm = getConnection().getMetaData();
            try (ResultSet tables = dbm.getTables(null, null, tableName, null)) {
                boolean exists = tables.next();
                Log.debug("Table '" + tableName + "' exists: " + exists);
                return exists;
            }
        } catch (SQLException e) {
            Log.error("Error checking if table '" + tableName + "' exists: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean columnExists(String tableName, String columnName) {
        validateIdentifier(tableName);
        validateIdentifier(columnName);
        try {
            DatabaseMetaData dbm = getConnection().getMetaData();
            try (ResultSet columns = dbm.getColumns(null, null, tableName, columnName)) {
                boolean exists = columns.next();
                Log.debug("Column '" + columnName + "' in table '" + tableName + "' exists: " + exists);
                return exists;
            }
        } catch (SQLException e) {
            Log.error("Error checking if column '" + columnName + "' exists in table '" + tableName + "': " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void addColumn(String tableName, String columnDefinition) {
        validateIdentifier(tableName);
        if (columnDefinition == null || columnDefinition.trim().isEmpty()) {
            throw new IllegalArgumentException("Column definition cannot be null or empty.");
        }

        String sql = String.format("ALTER TABLE %s ADD COLUMN %s", quoteIdentifier(tableName), columnDefinition);
        Log.debug("Executing DDL: " + sql);

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Column added to table '" + tableName + "'.");
        } catch (SQLException e) {
            Log.error("Failed to add column to table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void dropColumn(String tableName, String columnName) {
        validateIdentifier(tableName);
        validateIdentifier(columnName);

        String sql;
        if (databaseType == DatabaseType.SQLITE) {
            Log.warn("Dropping a column in SQLite requires recreating the table. This is a complex, potentially data-lossy operation and is not directly supported by this simplified method. Please handle this with care manually.");
            throw new UnsupportedOperationException("Dropping columns is not directly supported for SQLite due to its complexity.");
        } else {
            sql = String.format("ALTER TABLE %s DROP COLUMN %s", quoteIdentifier(tableName), quoteIdentifier(columnName));
        }

        Log.debug("Executing DDL: " + sql);
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Column '" + columnName + "' dropped from table '" + tableName + "'.");
        } catch (SQLException e) {
            Log.error("Failed to drop column '" + columnName + "' from table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void renameTable(String oldTableName, String newTableName) {
        validateIdentifier(oldTableName);
        validateIdentifier(newTableName);

        String sql = String.format("ALTER TABLE %s RENAME TO %s", quoteIdentifier(oldTableName), quoteIdentifier(newTableName));
        Log.debug("Executing DDL: " + sql);

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Table '" + oldTableName + "' renamed to '" + newTableName + "'.");
        } catch (SQLException e) {
            Log.error("Failed to rename table '" + oldTableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void renameColumn(String tableName, String oldColumnName, String newColumnName) {
        validateIdentifier(tableName);
        validateIdentifier(oldColumnName);
        validateIdentifier(newColumnName);

        String sql;
        if (databaseType == DatabaseType.SQLITE) {
            sql = String.format("ALTER TABLE %s RENAME COLUMN %s TO %s", quoteIdentifier(tableName), quoteIdentifier(oldColumnName), quoteIdentifier(newColumnName));
        } else if (databaseType == DatabaseType.MYSQL) {
            Log.warn("Renaming a column in MySQL requires specifying the full column definition. This method will attempt to retrieve it, but it may not be perfect for all column types.");
            String columnDefinition = getColumnDefinition(tableName, oldColumnName);
            sql = String.format("ALTER TABLE %s CHANGE COLUMN %s %s %s",
                    quoteIdentifier(tableName),
                    quoteIdentifier(oldColumnName),
                    quoteIdentifier(newColumnName),
                    columnDefinition);
        } else {
            throw new UnsupportedOperationException("Rename column not supported for " + databaseType);
        }

        Log.debug("Executing DDL: " + sql);
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Column '" + oldColumnName + "' in table '" + tableName + "' renamed to '" + newColumnName + "'.");
        } catch (SQLException e) {
            Log.error("Failed to rename column '" + oldColumnName + "' in table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void createIndex(String tableName, String indexName, List<String> columns) {
        validateIdentifier(tableName);
        validateIdentifier(indexName);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns for index cannot be null or empty.");
        }
        columns.forEach(this::validateIdentifier);

        String columnList = columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String sql = String.format("CREATE INDEX %s ON %s (%s)", quoteIdentifier(indexName), quoteIdentifier(tableName), columnList);

        Log.debug("Executing DDL: " + sql);
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Index '" + indexName + "' created on table '" + tableName + "'.");
        } catch (SQLException e) {
            Log.error("Failed to create index '" + indexName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void dropIndex(String indexName) {
        validateIdentifier(indexName);
        String sql = "DROP INDEX " + quoteIdentifier(indexName);

        if (databaseType == DatabaseType.MYSQL) {
            Log.error("In MySQL, you must specify the table name to drop an index. Use dropIndex(tableName, indexName).");
            throw new UnsupportedOperationException("For MySQL, use dropIndex(tableName, indexName).");
        }

        Log.debug("Executing DDL: " + sql);
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Index '" + indexName + "' dropped.");
        } catch (SQLException e) {
            Log.error("Failed to drop index '" + indexName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public void dropIndex(String tableName, String indexName) {
        validateIdentifier(tableName);
        validateIdentifier(indexName);

        if (databaseType != DatabaseType.MYSQL) {
            Log.warn("Specifying table name for dropping an index is specific to MySQL. For other databases like SQLite, it's ignored.");
            dropIndex(indexName);
            return;
        }

        String sql = String.format("DROP INDEX %s ON %s", quoteIdentifier(indexName), quoteIdentifier(tableName));
        Log.debug("Executing DDL: " + sql);

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            Log.info("Index '" + indexName + "' dropped from table '" + tableName + "'.");
        } catch (SQLException e) {
            Log.error("Failed to drop index '" + indexName + "' from table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public List<String> getTableNames() {
        List<String> tableNames = new ArrayList<>();
        try {
            DatabaseMetaData metaData = getConnection().getMetaData();
            String[] types = {"TABLE"};
            try (ResultSet rs = metaData.getTables(null, null, "%", types)) {
                while (rs.next()) {
                    tableNames.add(rs.getString("TABLE_NAME"));
                }
            }
            Log.debug("Retrieved table names: " + tableNames);
            return tableNames;
        } catch (SQLException e) {
            Log.error("Failed to retrieve table names: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, String>> getTableInfo(String tableName) {
        validateIdentifier(tableName);
        List<Map<String, String>> tableInfo = new ArrayList<>();
        try {
            DatabaseMetaData metaData = getConnection().getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
                while (columns.next()) {
                    Map<String, String> columnInfo = new LinkedHashMap<>();
                    columnInfo.put("COLUMN_NAME", columns.getString("COLUMN_NAME"));
                    columnInfo.put("TYPE_NAME", columns.getString("TYPE_NAME"));
                    columnInfo.put("COLUMN_SIZE", columns.getString("COLUMN_SIZE"));
                    columnInfo.put("IS_NULLABLE", columns.getString("IS_NULLABLE"));
                    columnInfo.put("IS_AUTOINCREMENT", columns.getString("IS_AUTOINCREMENT"));
                    tableInfo.add(columnInfo);
                }
            }
            Log.debug("Retrieved info for table '" + tableName + "'");
            return tableInfo;
        } catch (SQLException e) {
            Log.error("Failed to retrieve info for table '" + tableName + "': " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String getColumnDefinition(String tableName, String columnName) {
        validateIdentifier(tableName);
        validateIdentifier(columnName);
        try {
            DatabaseMetaData metaData = getConnection().getMetaData();
            try (ResultSet columns = metaData.getColumns(null, null, tableName, columnName)) {
                if (columns.next()) {
                    String typeName = columns.getString("TYPE_NAME");
                    int columnSize = columns.getInt("COLUMN_SIZE");
                    String isNullable = columns.getString("IS_NULLABLE");
                    String columnDef = columns.getString("COLUMN_DEF");

                    StringBuilder definition = new StringBuilder(typeName);
                    if (typeName.equalsIgnoreCase("VARCHAR") || typeName.equalsIgnoreCase("CHAR")) {
                        definition.append("(").append(columnSize).append(")");
                    }
                    if ("NO".equalsIgnoreCase(isNullable)) {
                        definition.append(" NOT NULL");
                    }
                    if (columnDef != null) {
                        definition.append(" DEFAULT ").append(columnDef);
                    }
                    Log.debug("Retrieved definition for '" + tableName + "." + columnName + "': " + definition);
                    return definition.toString();
                }
            }
        } catch (SQLException e) {
            Log.error("Could not retrieve column definition for '" + tableName + "." + columnName + "': " + e.getMessage());
        }
        throw new RuntimeException("Could not find column '" + columnName + "' in table '" + tableName + "'.");
    }

    private void bindParameters(PreparedStatement pstmt, List<?> params) throws SQLException {
        if (params != null) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }
        }
    }

    private String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return "";
        }
        if (databaseType == DatabaseType.MYSQL) {
            return "`" + identifier.replace("`", "``") + "`";
        } else {
            return '"' + identifier.replace("\"", "\"\"") + '"';
        }
    }

    private String buildUpsertSqlite(String tableName, List<String> conflictColumns, Map<String, Object> insertData) {
        String columns = insertData.keySet().stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = insertData.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));
        String conflictCols = conflictColumns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));

        String updateSet = insertData.keySet().stream()
                .filter(k -> !conflictColumns.contains(k))
                .map(k -> quoteIdentifier(k) + " = excluded." + quoteIdentifier(k))
                .collect(Collectors.joining(", "));

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s",
                quoteIdentifier(tableName), columns, placeholders, conflictCols, updateSet
        );
    }

    private String buildUpsertMysql(String tableName, List<String> conflictColumns, Map<String, Object> insertData) {
        String columns = insertData.keySet().stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = insertData.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));

        String updateSet = insertData.keySet().stream()
                .map(k -> quoteIdentifier(k) + " = VALUES(" + quoteIdentifier(k) + ")")
                .collect(Collectors.joining(", "));

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                quoteIdentifier(tableName), columns, placeholders, updateSet
        );
    }
}