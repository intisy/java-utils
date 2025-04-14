package io.github.intisy.utils.custom;

import io.github.intisy.simple.logger.EmptyLogger;
import io.github.intisy.simple.logger.SimpleLogger;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

@SuppressWarnings({"unused", "SqlNoDataSourceInspection", "SqlSourceToSinkFlow"})
public class SQL {

    private static final Pattern VALID_IDENTIFIER_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
    private static final int MAX_IDENTIFIER_LENGTH = 64;

    private final String url;
    private final String username;
    private final String password;
    private SimpleLogger logger;
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
                null, null, new EmptyLogger());
    }

    public SQL(String host, int port, String database, String username, String password) {
        this(String.format("jdbc:mysql://%s:%d/%s",
                        requireNonNull(host, "host cannot be null"),
                        port,
                        requireNonNull(database, "database cannot be null")),
                requireNonNull(username, "username cannot be null"),
                requireNonNull(password, "password cannot be null"),
                new EmptyLogger());
    }

    public SQL(String jdbcUrl, String username, String password, SimpleLogger logger) {
        this.url = requireNonNull(jdbcUrl, "jdbcUrl cannot be null");
        this.username = username;
        this.password = password;
        this.logger = requireNonNull(logger, "logger cannot be null");
        this.databaseType = detectDatabaseType(this.url);
        this.connection = initializeConnection();
        this.logger.debug("SQL connection initialized for " + this.url + " (Type: " + this.databaseType + ")");
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

    public void setLogger(SimpleLogger logger) {
        this.logger = requireNonNull(logger, "logger cannot be null");
    }

    public SimpleLogger getLogger() {
        return logger;
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
                    logger.debug("SQLite PRAGMA foreign_keys=ON set.");
                }
            }

            return conn;
        } catch (SQLException e) {
            logger.error("Failed to initialize database connection (" + url + "): " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            logger.warn("Connection was closed or null. Attempting to reconnect...");
            connection = initializeConnection();
        }
        return connection;
    }

    public void beginTransaction() throws SQLException {
        Connection conn = getConnection();
        if (autoCommit) {
            conn.setAutoCommit(false);
            autoCommit = false;
            logger.debug("Transaction started.");
        } else {
            logger.warn("beginTransaction called while already in a transaction.");
        }
    }

    public void commitTransaction() throws SQLException {
        Connection conn = getConnection();
        if (!autoCommit) {
            try {
                conn.commit();
                logger.debug("Transaction committed.");
            } catch (SQLException e) {
                logger.error("Transaction commit failed: " + e.getMessage());
                throw new RuntimeException(e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                    autoCommit = true;
                } catch (SQLException e) {
                    logger.error("Failed to reset autoCommit after commit: " + e.getMessage());
                }
            }
        } else {
            logger.warn("commitTransaction called without an active transaction.");
        }
    }

    public void rollbackTransaction() throws SQLException {
        Connection conn = getConnection();
        if (!autoCommit) {
            try {
                conn.rollback();
                logger.warn("Transaction rolled back.");
            } catch (SQLException e) {
                logger.error("Transaction rollback failed: " + e.getMessage());
                throw new RuntimeException(e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                    autoCommit = true;
                } catch (SQLException e) {
                    logger.error("Failed to reset autoCommit after rollback: " + e.getMessage());
                }
            }
        } else {
            logger.warn("rollbackTransaction called without an active transaction.");
        }
    }

    public void close() {
        if (connection != null) {
            try {
                if (!autoCommit) {
                    logger.warn("Closing connection with an uncommitted transaction. Rolling back.");
                    try {
                        connection.rollback();
                    } catch (SQLException ex) {
                        logger.error("Error rolling back transaction during close: " + ex.getMessage());
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
                    logger.debug("Database connection closed.");
                }
            } catch (SQLException e) {
                logger.error("Error closing database connection: " + e.getMessage());
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

    public void createTable(String tableName, String... columnDefs) {
        createTable(tableName, columnDefs, null);
    }

    public void createTable(String tableName, String[] columnDefs, String[] constraints) {
        validateIdentifier(tableName);
        if (columnDefs == null || columnDefs.length == 0) {
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
        logger.debug("Executing DDL: " + sqlString);

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sqlString);
            logger.info("Table '" + tableName + "' created or already exists.");
        } catch (SQLException e) {
            logger.error("Failed to create table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
    }

    public void deleteTable(String tableName) {
        validateIdentifier(tableName);
        String sql = "DROP TABLE IF EXISTS " + quoteIdentifier(tableName);
        logger.warn("Executing DDL (DROP TABLE): " + sql);

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            logger.info("Table '" + tableName + "' dropped or did not exist.");
        } catch (SQLException e) {
            logger.error("Failed to drop table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
    }

    public int insertData(String tableName, Object... columnsAndValues) {
        validateIdentifier(tableName);
        if (columnsAndValues.length == 0) {
            throw new IllegalArgumentException("No column/value pairs provided for insert.");
        }
        if (columnsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Columns and values must be provided in pairs.");
        }

        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < columnsAndValues.length; i += 2) {
            if (!(columnsAndValues[i] instanceof String)) {
                throw new IllegalArgumentException("Column name at index " + i + " must be a String.");
            }
            String colName = (String) columnsAndValues[i];
            validateIdentifier(colName);
            columns.add(quoteIdentifier(colName));
            values.add(columnsAndValues[i + 1]);
        }

        String sql = buildInsertStatement(tableName, columns);
        logger.debug("Executing insert: " + sql + " with " + values.size() + " values.");

        try (PreparedStatement pstmt = getConnection().prepareStatement(sql)) {
            bindParameters(pstmt, values);
            int affectedRows = pstmt.executeUpdate();
            logger.debug("Insert successful, " + affectedRows + " row(s) affected.");
            return affectedRows;
        } catch (SQLException e) {
            logger.error("Insert failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
            throw new RuntimeException(e);
        }
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
        List<Map<String, Object>> existing = selectData(tableName, new String[]{checkColumn}, where);

        if (existing.isEmpty()) {
            logger.debug("No existing record found for " + checkColumn + ". Inserting...");
            return insertData(tableName, columnsAndValues);
        } else {
            logger.debug("Skip insert - data already exists for " + checkColumn);
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
            logger.warn("updateData called with null whereClause - will update all rows in table '" + tableName + "'!");
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
            whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS ?" : " = ?"));
            whereValues.add(entry.getValue());
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

        logger.debug("Executing update: " + sqlString + " with " + allParams.size() + " parameters.");

        try (PreparedStatement pstmt = getConnection().prepareStatement(sqlString)) {
            bindParameters(pstmt, allParams);
            int affectedRows = pstmt.executeUpdate();
            logger.debug("Update successful, " + affectedRows + " row(s) affected.");
            return affectedRows;
        } catch (SQLException e) {
            logger.error("Update failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
    }

    public int deleteData(String tableName, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (whereClause == null) {
            whereClause = Collections.emptyMap();
            logger.warn("deleteData called with null whereClause - will delete all rows from table '" + tableName + "'!");
        }

        List<String> whereConditions = new ArrayList<>();
        List<Object> whereValues = new ArrayList<>();
        for (Map.Entry<String, Object> entry : whereClause.entrySet()) {
            validateIdentifier(entry.getKey());
            whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS ?" : " = ?"));
            whereValues.add(entry.getValue());
        }

        StringBuilder sql = new StringBuilder("DELETE FROM ")
                .append(quoteIdentifier(tableName));

        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        String sqlString = sql.toString();
        logger.debug("Executing delete: " + sqlString + " with " + whereValues.size() + " parameters.");

        try (PreparedStatement pstmt = getConnection().prepareStatement(sqlString)) {
            bindParameters(pstmt, whereValues);
            int affectedRows = pstmt.executeUpdate();
            logger.debug("Delete successful, " + affectedRows + " row(s) affected.");
            return affectedRows;
        } catch (SQLException e) {
            logger.error("Delete failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> selectData(String tableName, String[] columnsToSelect, Map<String, Object> whereClause) {
        validateIdentifier(tableName);
        if (columnsToSelect == null || columnsToSelect.length == 0) {
            throw new IllegalArgumentException("Must specify at least one column to select (or '*').");
        }

        String selectColsString;
        if (columnsToSelect.length == 1 && "*".equals(columnsToSelect[0])) {
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
            whereConditions.add(quoteIdentifier(entry.getKey()) + (entry.getValue() == null ? " IS ?" : " = ?"));
            whereValues.add(entry.getValue());
        }

        StringBuilder sql = new StringBuilder("SELECT ")
                .append(selectColsString)
                .append(" FROM ")
                .append(quoteIdentifier(tableName));

        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        String sqlString = sql.toString();
        logger.debug("Executing select: " + sqlString + " with " + whereValues.size() + " parameters.");

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
            logger.error("Select failed for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sqlString + "]");
            throw new RuntimeException(e);
        }
        logger.debug("Select executed, " + results.size() + " row(s) returned.");
        return results;
    }

    public List<Map<String, Object>> selectData(String tableName, String[] columnsToSelect, String whereColumn, Object whereValue) {
        Map<String, Object> whereClause = new LinkedHashMap<>();
        if (whereColumn != null) {
            validateIdentifier(whereColumn);
            whereClause.put(whereColumn, whereValue);
        }
        return selectData(tableName, columnsToSelect, whereClause);
    }

    public <T> List<T> selectSingleColumn(String tableName, String columnToSelect, Map<String, Object> whereClause, Class<T> expectedType) throws SQLException {
        validateIdentifier(columnToSelect);

        List<Map<String, Object>> rawResults = selectData(tableName, new String[]{columnToSelect}, whereClause);
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
                    logger.error("Type mismatch for column '" + columnToSelect + "'. Expected " + expectedType.getName() + ", but got " + value.getClass().getName() + ". Value: " + value);
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
        logger.debug("Preparing batch insert: " + sql);

        Connection conn = getConnection();
        boolean startedTransaction = false;
        int[] batchResults = {};

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            if (conn.getAutoCommit()) {
                conn.setAutoCommit(false);
                startedTransaction = true;
                logger.debug("Temporarily disabled autoCommit for batch insert.");
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
                logger.debug("Executing batch insert with " + batchCount + " statements.");
                batchResults = pstmt.executeBatch();
                logger.debug("Batch insert executed.");
            }

            if (startedTransaction) {
                conn.commit();
                logger.debug("Committed batch insert transaction.");
            }

        } catch (SQLException | IllegalArgumentException e) {
            logger.error("Batch insert failed for table '" + tableName + "': " + e.getMessage());
            if (startedTransaction) {
                try {
                    conn.rollback();
                    logger.warn("Rolled back batch insert transaction due to error.");
                } catch (SQLException rbEx) {
                    logger.error("Failed to rollback batch insert transaction: " + rbEx.getMessage());
                }
            }
            if (e instanceof SQLException) throw (SQLException) e;
            else throw new SQLException("Batch insert failed: " + e.getMessage(), e);
        } finally {
            if (startedTransaction) {
                try {
                    if (!conn.isClosed()) {
                        conn.setAutoCommit(true);
                        logger.debug("Restored autoCommit state.");
                    }
                } catch (SQLException acEx) {
                    logger.error("Failed to restore autoCommit state after batch insert: " + acEx.getMessage());
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

    private void bindParameters(PreparedStatement pstmt, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object param = params.get(i);
            if (param == null) {
                pstmt.setNull(i + 1, Types.VARCHAR);
            } else {
                pstmt.setObject(i + 1, param);
            }
        }
    }

    public void logDatabase() throws SQLException {
        logger.info("--- Logging Database Schema and Content (" + url + ") ---");
        DatabaseMetaData metaData = getConnection().getMetaData();
        String catalog = getConnection().getCatalog();
        String schemaPattern = (databaseType == DatabaseType.MYSQL) ? catalog : null;

        try (ResultSet tables = metaData.getTables(catalog, schemaPattern, "%", new String[]{"TABLE"})) {
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                logTable(tableName, metaData);
            }
        } catch (SQLException e) {
            logger.error("Failed to list tables: " + e.getMessage());
            throw new RuntimeException(e);
        }
        logger.info("--- Finished Logging Database ---");
    }

    private void logTable(String tableName, DatabaseMetaData metaData) throws SQLException {
        List<String> columns = getTableColumns(tableName, metaData);
        if (columns.isEmpty()) {
            logger.info("Table: " + tableName + " (No columns found or table does not exist)");
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
            logger.error("Failed to get columns for table '" + tableName + "': " + e.getMessage());
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
            logger.error("Failed to retrieve contents for table '" + tableName + "': " + e.getMessage() + " [SQL: " + sql + "]");
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

        logger.log("");
        logger.log(titleLine);
        logger.log(headerBuilder.toString());
        logger.log(dividerBuilder.toString());

        if (rows.isEmpty()) {
            String emptyRow = "| " + padRight("(No data)", totalWidth - 4) + " |";
            logger.log(emptyRow);
        } else {
            for (List<String> row : rows) {
                StringBuilder rowBuilder = new StringBuilder("|");
                for (int i = 0; i < columns.size(); i++) {
                    rowBuilder.append(" ").append(padRight(row.get(i), columnWidths[i])).append(" |");
                }
                logger.log(rowBuilder.toString());
            }
        }
        logger.log(dividerBuilder.toString());
        logger.log("");
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
}