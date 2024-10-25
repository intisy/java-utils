package io.github.intisy.utils.custom;

import io.github.intisy.simple.logger.EmptyLogger;
import io.github.intisy.simple.logger.SimpleLogger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class SQL {
    String url;
    String username;
    String password;
    SimpleLogger logger = new EmptyLogger();
    Connection connection;
    public SQL(String url) {
        setUrl(url);
    }
    public SQL(String url, String username, String password) {
        overwriteConnection(url, username, password);
    }
    private Connection getConnection() {
        try {
            if (username != null && password != null)
                return DriverManager.getConnection(url, username, password);
            else
                return DriverManager.getConnection(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void overwriteConnection(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }
    public void setPassword(String PASSWORD) {
        this.password = PASSWORD;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void logEntireDatabase() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                System.out.println("\n=== Table: " + tableName + " ===");
                List<String> columns = new ArrayList<>();
                try (ResultSet cols = metaData.getColumns(null, null, tableName, null)) {
                    while (cols.next()) {
                        columns.add(cols.getString("COLUMN_NAME"));
                    }
                }
                System.out.println(String.join(" | ", columns));
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < columns.size() * 20; i++) {
                    sb.append("-");
                }
                System.out.println(sb);

                // Query and print all rows
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {

                    while (rs.next()) {
                        StringBuilder row = new StringBuilder();
                        for (String column : columns) {
                            if (row.length() > 0) row.append(" | ");
                            String value = rs.getString(column);
                            row.append(value == null ? "NULL" : value);
                        }
                        System.out.println(row);
                    }
                }
            }
        }
    }

    public void createTable(String name, String... args) {
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {
            StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + name + " (");
            for (int i = 0; i < args.length; i++) {
                sql.append(args[i]);
                if (i < args.length - 1) {
                    sql.append(", ");
                }
            }
            sql.append(")");
            executeStatement(statement, sql.toString());
            logger.debug("Table created successfully.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void deleteTable(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;

        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(sql);
            logger.debug("Table '" + tableName + "' deleted successfully.");
        } catch (SQLException e) {
            logger.exception(e);
        }
    }
    public void executeStatement(Statement statement, String sql) {
        logger.debug("Executing SQL Statement: " + sql);
        try {
            statement.execute(sql);
        } catch (Exception ignored) {}
    }

    public void setLogger(SimpleLogger logger) {
        this.logger = logger;
    }
    public void displayTableData(String tableName) {
        String sql = "SELECT * FROM " + tableName;

        try (Connection connection = getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Print column names
            for (int i = 1; i <= columnCount; i++) {
                logger.debug(metaData.getColumnName(i) + "\t");
            }
            logger.debug("");

            // Print table data
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    logger.debug(resultSet.getString(i) + "\t");
                }
                logger.debug("");
            }
        } catch (SQLException e) {
            logger.exception(e);
        }
    }
    public List<String> selectData(String name, String select, String... args) {
        StringBuilder sql = new StringBuilder("SELECT " + select + " FROM " + name + " WHERE ");
        for (int i = 0; i < args.length; i += 2) {
            sql.append(args[i]).append(" = ?");
            if (i < args.length - 2) {
                sql.append(" AND ");
            }
        }
        logger.debug("Executing SQL Statement: " + sql);
        List<String> result = new ArrayList<>();
        try (Connection connection = getConnection()) {
            PreparedStatement statement = prepareStatement(connection, sql.toString());
            for (int i = 2; i <= args.length; i += 2) {
                statement.setString(i / 2, args[i - 1]);
            }
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(select));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private PreparedStatement prepareStatement(Connection connection, String sql) {
        // You can cache prepared statements here
        try {
            return connection.prepareStatement(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void insertData(String name, String... args) {
        StringBuilder sql = new StringBuilder("INSERT INTO " + name + " (");
        for (int i = 0; i < args.length; i+=2) {
            sql.append(args[i]);
            if (i < args.length - 2) {
                sql.append(", ");
            }
        }
        sql.append(") VALUES (");
        for (int i = 0; i < args.length; i+=2) {
            sql.append("?");
            if (i < args.length - 2) {
                sql.append(", ");
            }
        }
        sql.append(")");
        logger.debug("Executing SQL Statement: " + sql);
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     sql.toString())) {
            for (int i = 2; i <= args.length; i+=2) {
                statement.setString(i/2, args[i-1]);
            }
            int rowsInserted = statement.executeUpdate();
            if (rowsInserted > 0) {
                logger.debug("A new " + name.substring(0, name.length()-1) + " was inserted successfully!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void updateData(String name, String primaryKey, String primaryKeyValue, String... args) {
        StringBuilder sql = new StringBuilder("UPDATE " + name + " SET ");
        for (int i = 0; i < args.length; i += 2) {
            sql.append(args[i]).append(" = ?");
            if (i < args.length - 2) {
                sql.append(", ");
            }
        }
        sql.append(" WHERE ").append(primaryKey).append(" = ?");

        logger.debug("Executing SQL Statement: " + sql);
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int paramIndex = 1;
            for (int i = 1; i < args.length; i += 2) {
                statement.setString(paramIndex++, args[i]);
            }
            statement.setString(paramIndex, primaryKeyValue);

            int rowsUpdated = statement.executeUpdate();
            if (rowsUpdated > 0) {
                logger.debug("Data in " + name + " table updated successfully!");
            } else {
                logger.debug("No rows were updated.");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
