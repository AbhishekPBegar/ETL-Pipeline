package com.etl.pipeline.service;

import com.etl.pipeline.config.DatabaseConfig;
import com.etl.pipeline.model.User;
import com.etl.pipeline.util.ETLLogger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class LoadingService {
    
    public void loadUsers(List<User> users) {
        if (users.isEmpty()) {
            ETLLogger.logMessage("No users to load into database");
            return;
        }
        
        String insertSQL = "INSERT INTO users (name, age, city, source) VALUES (?, ?, ?, ?)";
        
        try (Connection connection = DatabaseConfig.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            
            int batchSize = 0;
            for (User user : users) {
                preparedStatement.setString(1, user.getName());
                preparedStatement.setObject(2, user.getAge());
                preparedStatement.setString(3, user.getCity());
                preparedStatement.setString(4, user.getSource());
                
                preparedStatement.addBatch();
                batchSize++;
                
                // Execute batch every 100 records for better performance
                if (batchSize % 100 == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }
            }
            
            // Execute remaining batch
            if (batchSize % 100 != 0) {
                preparedStatement.executeBatch();
            }
            
            ETLLogger.logMessage("Successfully loaded " + users.size() + " users into database");
            
        } catch (SQLException e) {
            ETLLogger.logError("Failed to load users into database", e);
        }
    }
}
