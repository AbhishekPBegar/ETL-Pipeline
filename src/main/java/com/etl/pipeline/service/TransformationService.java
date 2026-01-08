package com.etl.pipeline.service;

import com.etl.pipeline.model.User;
import com.etl.pipeline.util.ETLLogger;

import java.util.List;
import java.util.stream.Collectors;

public class TransformationService {
    
    public List<User> transformUsers(List<User> users, String source) {
        ETLLogger.logMessage("Starting transformation for " + users.size() + " records from " + source);
        
        List<User> transformedUsers = users.stream()
                .filter(this::isValidUser)
                .map(this::standardizeUser)
                .collect(Collectors.toList());
        
        ETLLogger.logMessage("Transformation completed. " + transformedUsers.size() + " valid records from " + source);
        
        return transformedUsers;
    }
    
    private boolean isValidUser(User user) {
        // Basic validation - ensure name is not empty
        return user.getName() != null && !user.getName().trim().isEmpty();
    }
    
    private User standardizeUser(User user) {
        // Standardize data format
        User standardizedUser = new User();
        standardizedUser.setName(user.getName().trim());
        standardizedUser.setAge(user.getAge());
        standardizedUser.setCity(user.getCity() != null ? user.getCity().trim() : "Unknown");
        standardizedUser.setSource(user.getSource());
        
        return standardizedUser;
    }
}
