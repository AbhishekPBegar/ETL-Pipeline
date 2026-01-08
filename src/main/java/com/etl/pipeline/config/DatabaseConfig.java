package com.etl.pipeline.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);
    private static final String DB_URL = "jdbc:sqlite:database.db";
    
    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
            Connection connection = DriverManager.getConnection(DB_URL);
            logger.info("Connected to SQLite database successfully");
            return connection;
        } catch (ClassNotFoundException e) {
            logger.error("SQLite JDBC driver not found", e);
            throw new SQLException("Database driver not found", e);
        }
    }
    
    public static void initializeDatabase() {
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {
            
            String createTableSQL = 
                "CREATE TABLE IF NOT EXISTS users (\n" +
                "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                "    name TEXT NOT NULL,\n" +
                "    age INTEGER,\n" +
                "    city TEXT,\n" +
                "    source TEXT NOT NULL,\n" +
                "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n" +
                ")";
            
            statement.execute(createTableSQL);
            logger.info("Database table 'users' initialized successfully");
            
        } catch (SQLException e) {
            logger.error("Failed to initialize database", e);
            throw new RuntimeException("Database initialization failed", e);
        }
    }
}
