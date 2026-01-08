package com.etl.pipeline.service;

import com.etl.pipeline.model.User;
import com.etl.pipeline.util.ETLLogger;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ExtractionService {
    
    public List<User> extractFromCSV(String filePath) {
        List<User> users = new ArrayList<>();
        
        try (FileReader fileReader = new FileReader(filePath);
             CSVReader csvReader = new CSVReaderBuilder(fileReader)
                     .withSkipLines(1)
                     .build()) {
            
            String[] nextRecord;
            while ((nextRecord = csvReader.readNext()) != null) {
                if (nextRecord.length >= 3) {
                    String name = nextRecord[0].trim();
                    Integer age = parseAge(nextRecord[1].trim());
                    String city = nextRecord[2].trim();
                    
                    users.add(new User(name, age, city, "csv"));
                }
            }
            
            ETLLogger.logMessage("Successfully extracted " + users.size() + " records from CSV: " + filePath);
            
        } catch (Exception e) {
            ETLLogger.logError("Failed to extract data from CSV file: " + filePath, e);
        }
        
        return users;
    }
    
    public List<User> extractFromJSON(String filePath) {
        List<User> users = new ArrayList<>();
        
        try {
            StringBuilder jsonContent = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    jsonContent.append(line);
                }
            }
            
            JSONArray jsonArray = new JSONArray(jsonContent.toString());
            
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                
                String name = jsonObject.getString("name");
                Integer age = parseAge(jsonObject.optString("age", "0"));
                String city = jsonObject.getString("city");
                
                users.add(new User(name, age, city, "json"));
            }
            
            ETLLogger.logMessage("Successfully extracted " + users.size() + " records from JSON: " + filePath);
            
        } catch (Exception e) {
            ETLLogger.logError("Failed to extract data from JSON file: " + filePath, e);
        }
        
        return users;
    }
    
    public List<User> extractFromAPI(String apiUrl) {
        List<User> users = new ArrayList<>();
        
        try {
            URL url = java.net.URI.create(apiUrl).toURL();
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            
            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
                
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                
                JSONArray jsonArray = new JSONArray(response.toString());
                
                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    
                    String name = jsonObject.getString("name");
                    Integer age = parseAge(jsonObject.optString("age", "25"));
                    String city = jsonObject.optJSONObject("address") != null ? 
                                 jsonObject.getJSONObject("address").getString("city") : "Unknown";
                    
                    users.add(new User(name, age, city, "api"));
                }
                
                ETLLogger.logMessage("Successfully extracted " + users.size() + " records from API: " + apiUrl);
                
            } else {
                ETLLogger.logError("API request failed with response code: " + responseCode, null);
            }
            
        } catch (Exception e) {
            ETLLogger.logError("Failed to extract data from API: " + apiUrl, e);
        }
        
        return users;
    }
    
    private Integer parseAge(String ageStr) {
        try {
            return Integer.parseInt(ageStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
