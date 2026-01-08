package com.etl.pipeline;

import com.etl.pipeline.config.DatabaseConfig;
import com.etl.pipeline.model.User;
import com.etl.pipeline.service.ExtractionService;
import com.etl.pipeline.service.LoadingService;
import com.etl.pipeline.service.TransformationService;
import com.etl.pipeline.util.ETLLogger;

import java.util.List;

public class ETLPipelineApplication {

    private final ExtractionService extractionService;
    private final TransformationService transformationService;
    private final LoadingService loadingService;

    public ETLPipelineApplication() {
        this.extractionService = new ExtractionService();
        this.transformationService = new TransformationService();
        this.loadingService = new LoadingService();
    }

    public void runETL() {
        ETLLogger.logMessage("ETL Job Started.");

        try {
            // Initialize database
            DatabaseConfig.initializeDatabase();

            // Extract data from all sources
            List<User> csvUsers = extractionService.extractFromCSV("data/sample_data.csv");
            List<User> jsonUsers = extractionService.extractFromJSON("data/sample_data.json");
            List<User> apiUsers = extractionService.extractFromAPI("https://jsonplaceholder.typicode.com/users");

            // Transform data
            List<User> transformedCsvUsers = transformationService.transformUsers(csvUsers, "csv");
            List<User> transformedJsonUsers = transformationService.transformUsers(jsonUsers, "json");
            List<User> transformedApiUsers = transformationService.transformUsers(apiUsers, "api");

            // Load data
            loadingService.loadUsers(transformedCsvUsers);
            loadingService.loadUsers(transformedJsonUsers);
            loadingService.loadUsers(transformedApiUsers);

            // Summary
            int totalProcessed = transformedCsvUsers.size() + transformedJsonUsers.size() + transformedApiUsers.size();
            ETLLogger.logMessage("ETL Job Completed Successfully. Total records processed: " + totalProcessed);

        } catch (Exception e) {
            ETLLogger.logError("ETL Job Failed", e);
            throw new RuntimeException("ETL Pipeline execution failed", e);
        }

        ETLLogger.logMessage("ETL Job Completed.\n");
    }

    public static void main(String[] args) {
        ETLPipelineApplication app = new ETLPipelineApplication();
        app.runETL();
    }
}
// PR Testing