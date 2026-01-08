package com.etl.pipeline;

import com.etl.pipeline.config.DatabaseConfig;
import com.etl.pipeline.util.ETLLogger;

public class TestDatabaseSetup {
    public static void main(String[] args) {
        try {
            ETLLogger.logMessage("Testing database setup...");
            DatabaseConfig.initializeDatabase();
            ETLLogger.logMessage("Database setup test completed successfully!");
        } catch (Exception e) {
            ETLLogger.logError("Database setup test failed", e);
        }
    }
}
