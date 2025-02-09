package io.github.intisy.utils.custom;

import io.github.intisy.simple.logger.EmptyLogger;
import io.github.intisy.simple.logger.SimpleLogger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused")
public class SQL implements AutoCloseable {
    private static final Log log = LogFactory.getLog(SQL.class);
    private final String url;
    private String username;
    private String password;
    private SimpleLogger logger;
    private final DatabaseType databaseType;
    private Connection connection;
    private static final List<Doublet<String, String>> CREATE_TABLE_TEMPLATES = new ArrayList<>();
    static {
        CREATE_TABLE_TEMPLATES.add(new Doublet<>("INTEGER", "INT"));
        CREATE_TABLE_TEMPLATES.add(new Doublet<>("AUTOINCREMENT", "AUTO_INCREMENT"));
    }

    public enum DatabaseType {
        SQLITE,
        MYSQL,
        UNKNOWN
    }

    public enum Type {
        NORMAL,
        UPDATE,
        QUERY,
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
    private SQL(String url, String username, String password, SimpleLogger logger) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.logger = logger;
        this.databaseType = detectDatabaseType();
        this.connection = initializeConnection();
    }

    public void setPassword(String password) {
        this.password = password;
        this.connection = initializeConnection();
    }

    public void setUsername(String username) {
        this.username = username;
        this.connection = initializeConnection();
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public void setLogger(SimpleLogger logger) {
        this.logger = logger;
    }

    public SimpleLogger getLogger() {
        return logger;
    }

    public void deleteData(String tableName, String... whereClause) {
        String sql = buildDeleteStatement(tableName, whereClause);
        try (Statement statement = getConnection().createStatement()) {
            executeSQL(statement, sql, "delete data", Type.UPDATE);
        } catch (SQLException e) {
            logger.error("Delete failed: " + e.getMessage());
            throw new RuntimeException("Failed to delete data", e);
        }
    }

    private String buildDeleteStatement(String tableName, String... whereClause) {
        StringBuilder sql = new StringBuilder("DELETE FROM ")
                .append(tableName);

        if (whereClause.length > 0) {
            if (whereClause.length % 2 != 0) {
                throw new IllegalArgumentException("WHERE clause parameters must be in column-value pairs.");
            }

            sql.append(" WHERE ");
            for (int i = 0; i < whereClause.length; i += 2) {
                if (i > 0) sql.append(" AND ");

                sql.append(whereClause[i]);

                // Check if the value is null and append accordingly
                if (whereClause[i + 1] == null) {
                    sql.append(" IS NULL");
                } else {
                    sql.append(" = '").append(whereClause[i + 1].replace("'", "''")).append("'");
                }
            }
        }

        return sql.toString();
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
                try (Statement statement = conn.createStatement()) {
                    statement.execute("PRAGMA foreign_keys = ON");
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
            executeSQL(statement, sql, "delete table", Type.NORMAL);
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
            executeSQL(statement, sql, "create table", Type.NORMAL);
        } catch (SQLException e) {
            logger.error("Failed to create table: " + e.getMessage());
            throw new RuntimeException("Table creation failed", e);
        }
    }

    public ResultSet executeSQL(Statement statement, String sql, String name, Type type) {
        try {
            logger.debug("Executing " + name + ": \"" + sql + "\" with type " + type);
            switch (type) {
                case NORMAL:
                    statement.execute(sql);
                    logger.debug("Executed " + name + " successfully: " + sql);
                    return null;
                case UPDATE:
                    int rowsAffected = statement.executeUpdate(sql);
                    logger.debug("Executed " + name + " successfully with " + rowsAffected + " affected rows");
                    return null;
                case QUERY:
                    return statement.executeQuery(sql);
                default:
                    return null;
            }
        } catch (SQLException e) {
            logger.error("Failed to " + name + ": " + e.getMessage());
            throw new RuntimeException("SQL execution failed", e);
        }
    }

    public void insertDataIfEmpty(String tableName, Object... columnsAndValues) {
        if (columnsAndValues.length < 2) {
            throw new IllegalArgumentException("Must provide at least one column-value pair");
        }

        // Check if data exists using first column-value pair
        List<String> existing = selectData(tableName, columnsAndValues[0].toString(),
                columnsAndValues[0].toString(), columnsAndValues[1].toString());

        if (existing.isEmpty()) {
            insertData(tableName, columnsAndValues);
            logger.debug("Inserted new record as no matching data found");
        } else {
            logger.debug("Skip insert - data already exists");
        }
    }

    public void insertData(String tableName, Object... columnsAndValues) {
        if (columnsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Columns and values must be paired");
        }

        String sql = buildInsertStatement(tableName, columnsAndValues);
        try (Statement statement = getConnection().createStatement()) {
            executeSQL(statement, sql, "insert data", Type.UPDATE);
        } catch (SQLException e) {
            logger.error("Insert failed: " + e.getMessage());
            throw new RuntimeException("Failed to insert data", e);
        }
    }

    public List<String> selectData(String tableName, String columnToSelect, String... whereClause) {
        String sql = buildSelectStatement(tableName, columnToSelect, whereClause);
        List<String> results = new ArrayList<>();
        try (Statement statement = getConnection().createStatement()) {
            try (ResultSet rs = executeSQL(statement, sql, "select data", Type.QUERY)) {
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

    public void updateData(String tableName, String primaryKey, String primaryKeyValue, String... columnsAndValues) {
        String sql = buildUpdateStatement(tableName, primaryKey, primaryKeyValue, columnsAndValues);
        List<String> results = new ArrayList<>();
        try (Statement statement = getConnection().createStatement()) {
            executeSQL(statement, sql, "update data", Type.UPDATE);
        } catch (SQLException e) {
            logger.error("Update failed: " + e.getMessage());
            throw new RuntimeException("Failed to update data", e);
        }
    }

    public void logDatabase() {
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

    public void logTable(String tableName) throws SQLException {
        logTable(tableName, getConnection().getMetaData());
    }
    public void logTable(String tableName, DatabaseMetaData metaData) throws SQLException {
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
        try (Statement statement = getConnection().createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
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
        String title = " Table: " + tableName + " ";
        String divider = String.join("", Collections.nCopies(Math.max((lines.get(0).length() - title.length()) / 2, 3), "-"));
        String combined = divider + title + divider;
        combined += combined.length() < lines.get(0).length() ? "-" : "";
        System.out.println("\n");
        logger.log(combined);
        for (String line : lines)
            logger.log(line);
        logger.log(String.join("", Collections.nCopies(combined.length(), "-")));
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = initializeConnection();
        }
        return connection;
    }

    private String buildCreateTableStatement(String name, String... columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(name)
                .append(" (");

        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            for (Doublet<String, String> template : CREATE_TABLE_TEMPLATES) {
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

    private String buildInsertStatement(String tableName, Object... columnsAndValues) {
        if (columnsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Columns and values must be paired.");
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (int i = 0; i < columnsAndValues.length; i += 2) {
            columns.append(columnsAndValues[i]).append(", ");

            // Check if value is null and append accordingly
            if (columnsAndValues[i + 1] == null) {
                values.append("NULL, ");
            } else {
                values.append("'").append(columnsAndValues[i + 1].toString().replace("'", "''")).append("', ");
            }
        }

        // Remove trailing commas
        columns.setLength(columns.length() - 2);
        values.setLength(values.length() - 2);

        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
    }

    private String buildSelectStatement(String tableName, String columnToSelect, String... whereClause) {
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(columnToSelect)
                .append(" FROM ")
                .append(tableName);

        if (whereClause.length > 0) {
            if (whereClause.length % 2 != 0) {
                throw new IllegalArgumentException("WHERE clause parameters must be in column-value pairs.");
            }

            sql.append(" WHERE ");
            for (int i = 0; i < whereClause.length; i += 2) {
                if (i > 0) sql.append(" AND ");

                sql.append(whereClause[i]);

                // Check if the value is null and append accordingly
                if (whereClause[i + 1] == null) {
                    sql.append(" IS NULL");
                } else {
                    sql.append(" = '").append(whereClause[i + 1].replace("'", "''")).append("'");
                }
            }
        }

        return sql.toString();
    }

    private String buildUpdateStatement(String tableName, String primaryKey, String primaryKeyValue, String... columnsAndValues) {
        if (columnsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Columns and values must be paired.");
        }

        StringBuilder sql = new StringBuilder("UPDATE ")
                .append(tableName)
                .append(" SET ");

        for (int i = 0; i < columnsAndValues.length; i += 2) {
            if (i > 0) sql.append(", ");

            sql.append(columnsAndValues[i]).append(" = ");

            // Check if value is null and append accordingly
            if (columnsAndValues[i + 1] == null) {
                sql.append("NULL");
            } else {
                sql.append("'").append(columnsAndValues[i + 1].replace("'", "''")).append("'");
            }
        }

        sql.append(" WHERE ").append(primaryKey).append(" = '").append(primaryKeyValue.replace("'", "''")).append("'");
        return sql.toString();
    }

    public void createTable(String tableName, String[] constraints, String... columns) {
        String sql = buildCreateTableStatement(tableName, constraints, columns);

        try (Statement statement = this.getConnection().createStatement()) {
            this.executeSQL(statement, sql, "create table", SQL.Type.NORMAL);
        } catch (SQLException e) {
            this.logger.error("Failed to create table: " + e.getMessage());
            throw new RuntimeException("Table creation failed", e);
        }
    }

    private String buildCreateTableStatement(String tableName, String[] constraints, String... columns) {
        if (constraints == null || constraints.length == 0)
            return buildCreateTableStatement(tableName, columns);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        for (String column : columns) {
            sb.append(column);
            sb.append(", ");
        }
        for (int i = 0; i < constraints.length; i++) {
            sb.append(constraints[i]);
            if (i < constraints.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}