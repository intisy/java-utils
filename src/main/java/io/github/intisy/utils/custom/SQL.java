package io.github.intisy.utils.custom;

import io.github.intisy.simple.logger.EmptyLogger;
import io.github.intisy.simple.logger.SimpleLogger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused")
public class SQL implements AutoCloseable {
    private final String url;
    private final String username;
    private final String password;
    private SimpleLogger logger;
    private final DatabaseType databaseType;
    private Connection connection;
    private static final List<Double<String, String>> CREATE_TABLE_TEMPLATES = new ArrayList<>();
    static {
        CREATE_TABLE_TEMPLATES.add(new Double<>("INTEGER", "INT"));
        CREATE_TABLE_TEMPLATES.add(new Double<>("AUTOINCREMENT", "AUTO_INCREMENT"));
    }

    public enum DatabaseType {
        SQLITE,
        MYSQL,
        UNKNOWN
    }

    // Constructor for SQLite
    public SQL(String url) {
        this(url, null, null, new EmptyLogger());
    }

    // Constructor for MySQL
    public SQL(String url, String username, String password) {
        this(url, username, password, new EmptyLogger());
    }

    // Full constructor
    public SQL(String url, String username, String password, SimpleLogger logger) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.logger = logger;
        this.databaseType = detectDatabaseType();
        this.connection = initializeConnection();
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public void setLogger(SimpleLogger logger) {
        this.logger = logger;
    }

    private DatabaseType detectDatabaseType() {
        if (url.toLowerCase().contains("sqlite")) {
            return DatabaseType.SQLITE;
        } else if (url.toLowerCase().contains("mysql")) {
            return DatabaseType.MYSQL;
        }
        return DatabaseType.UNKNOWN;
    }

    private Connection initializeConnection() {
        try {
            Connection conn = username != null && password != null
                    ? DriverManager.getConnection(url, username, password)
                    : DriverManager.getConnection(url);

            // Database-specific initialization
            if (databaseType == DatabaseType.SQLITE) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("PRAGMA foreign_keys = ON");
                }
            }

            return conn;
        } catch (SQLException e) {
            logger.error("Failed to initialize database connection: " + e.getMessage());
            throw new RuntimeException("Database connection failed", e);
        }
    }

    public void deleteTable(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        try (Statement statement = getConnection().createStatement()) {
            executeSQL(statement, sql, "delete table");
        } catch (SQLException e) {
            logger.error("Failed to delete table: " + e.getMessage());
            throw new RuntimeException("Table creation failed", e);
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("Error closing database connection: " + e.getMessage());
        }
    }

    public void createTable(String name, String... columns) {
        String sql = buildCreateTableStatement(name, columns);
        try (Statement statement = getConnection().createStatement()) {
            executeSQL(statement, sql, "create table");
        } catch (SQLException e) {
            logger.error("Failed to create table: " + e.getMessage());
            throw new RuntimeException("Table creation failed", e);
        }
    }

    public void executeSQL(Statement statement, String sql, String name) {
        try {
            logger.debug("Executing " + name + ": " + sql);
            statement.execute(sql);
            logger.debug("Executed " + name + " successfully: " + sql);
        } catch (SQLException e) {
            logger.error("Failed to " + name + ": " + e.getMessage());
            throw new RuntimeException("SQL execution failed", e);
        }
    }

    private String buildCreateTableStatement(String name, String... columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(name)
                .append(" (");

        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            for (Double<String, String> template : CREATE_TABLE_TEMPLATES) {
                if (databaseType == DatabaseType.MYSQL) {
                    column = column.replace(template.getKey(), template.getValue());
                } else if (databaseType == DatabaseType.SQLITE) {
                    column = column.replace(template.getValue(), template.getKey());
                }
            }
            sql.append(column);
            if (i < columns.length - 1) {
                sql.append(", ");
            }
        }
        return sql.append(")").toString();
    }

    public void insertDataIfEmpty(String tableName, String... columnsAndValues) {
        if (columnsAndValues.length < 2) {
            throw new IllegalArgumentException("Must provide at least one column-value pair");
        }

        // Check if data exists using first column-value pair
        List<String> existing = selectData(tableName, columnsAndValues[0],
                columnsAndValues[0], columnsAndValues[1]);

        if (existing.isEmpty()) {
            insertData(tableName, columnsAndValues);
            logger.debug("Inserted new record as no matching data found");
        } else {
            logger.debug("Skip insert - data already exists");
        }
    }

    public void insertData(String tableName, String... columnsAndValues) {
        if (columnsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Columns and values must be paired");
        }

        String sql = buildInsertStatement(tableName, columnsAndValues);

        try (PreparedStatement stmt = prepareStatement(sql)) {
            for (int i = 1; i <= columnsAndValues.length / 2; i++) {
                stmt.setString(i, columnsAndValues[i * 2 - 1]);
            }

            int rowsAffected = stmt.executeUpdate();
            logger.debug("Inserted " + rowsAffected + " rows into " + tableName);
        } catch (SQLException e) {
            logger.error("Insert failed: " + e.getMessage());
            throw new RuntimeException("Failed to insert data", e);
        }
    }

    public List<String> selectData(String tableName, String columnToSelect, String... whereClause) {
        String sql = buildSelectStatement(tableName, columnToSelect, whereClause);
        List<String> results = new ArrayList<>();

        try (PreparedStatement stmt = prepareStatement(sql)) {
            for (int i = 0; i < whereClause.length / 2; i++) {
                stmt.setString(i + 1, whereClause[i * 2 + 1]);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(rs.getString(columnToSelect));
                }
            }
        } catch (SQLException e) {
            logger.error("Select failed: " + e.getMessage());
            throw new RuntimeException("Failed to select data", e);
        }

        return results;
    }

    public void updateData(String tableName, String primaryKey, String primaryKeyValue,
                           String... columnsAndValues) {
        String sql = buildUpdateStatement(tableName, primaryKey, columnsAndValues);

        try (PreparedStatement stmt = prepareStatement(sql)) {
            int paramIndex = 1;
            for (int i = 1; i < columnsAndValues.length; i += 2) {
                stmt.setString(paramIndex++, columnsAndValues[i]);
            }
            stmt.setString(paramIndex, primaryKeyValue);

            int rowsAffected = stmt.executeUpdate();
            logger.debug("Updated " + rowsAffected + " rows in " + tableName);
        } catch (SQLException e) {
            logger.error("Update failed: " + e.getMessage());
            throw new RuntimeException("Failed to update data", e);
        }
    }

    public void logEntireDatabase() {
        try {
            DatabaseMetaData metaData = getConnection().getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    logTable(tables.getString("TABLE_NAME"), metaData);
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to log database: " + e.getMessage());
            throw new RuntimeException("Database logging failed", e);
        }
    }

    private void logTable(String tableName, DatabaseMetaData metaData) throws SQLException {
        List<String> columns = getTableColumns(tableName, metaData);
        logTableContents(tableName, columns);
    }

    private List<String> getTableColumns(String tableName, DatabaseMetaData metaData)
            throws SQLException {
        List<String> columns = new ArrayList<>();
        try (ResultSet cols = metaData.getColumns(null, null, tableName, null)) {
            while (cols.next()) {
                columns.add(cols.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    private void logTableContents(String tableName, List<String> columns) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        String sql = "SELECT * FROM " + tableName;
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (String column : columns) {
                    String value = rs.getString(column);
                    row.add(value == null ? "NULL" : value);
                }
                rows.add(row);
            }
        }
        List<String> lines = new ArrayList<>();
        int index = 0;
        for (String column : columns) {
            int length = column.length();
            for (List<String> row : rows) {
                length = Math.max(length, row.get(index).length());
            }
            String header = column + String.join("", Collections.nCopies(length - column.length(), " "));
            String divider = String.join("", Collections.nCopies(length, "-"));
            List<String> content = new ArrayList<>();
            for (List<String> row : rows) {
                content.add(row.get(index) + String.join("", Collections.nCopies(length - row.get(index).length(), " ")));
            }
            if (lines.isEmpty()) {
                lines.add(header);
                lines.add(divider);
                lines.addAll(content);
            } else {
                lines.set(0, lines.get(0) + " | " + header);
                lines.set(1, lines.get(1) + "---" + divider);
                for (int i = 2; i - 2 < content.size(); i++) {
                    lines.set(i, lines.get(i) + " | " + content.get(i - 2));
                }
            }
            index++;
        }
        String title = "Table: " + tableName;
        String divider = String.join("", Collections.nCopies(Math.max((lines.get(0).length() - title.length()) / 2, 0), "-"));
        logger.log("\n" + divider + title + divider);
        for (String line : lines)
            logger.log(line);
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = initializeConnection();
        }
        return connection;
    }

    private PreparedStatement prepareStatement(String sql) throws SQLException {
        return getConnection().prepareStatement(sql);
    }

    private String buildInsertStatement(String tableName, String... columnsAndValues) {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (int i = 0; i < columnsAndValues.length; i += 2) {
            if (i > 0) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append(columnsAndValues[i]);
            values.append("?");
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, columns, values);
    }

    private String buildSelectStatement(String tableName, String columnToSelect,
                                        String... whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(columnToSelect)
                .append(" FROM ")
                .append(tableName);

        if (whereClause.length > 0) {
            sql.append(" WHERE ");
            for (int i = 0; i < whereClause.length; i += 2) {
                if (i > 0) sql.append(" AND ");
                sql.append(whereClause[i]).append(" = ?");
            }
        }

        return sql.toString();
    }

    private String buildUpdateStatement(String tableName, String primaryKey,
                                        String... columnsAndValues) {
        StringBuilder sql = new StringBuilder("UPDATE ")
                .append(tableName)
                .append(" SET ");

        for (int i = 0; i < columnsAndValues.length; i += 2) {
            if (i > 0) sql.append(", ");
            sql.append(columnsAndValues[i]).append(" = ?");
        }

        sql.append(" WHERE ").append(primaryKey).append(" = ?");
        return sql.toString();
    }
}