package com.etl.pipeline.util;

// package com.etl.pipeline.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ETLLogger {
    private static final Logger logger = LoggerFactory.getLogger(ETLLogger.class);
    private static final String LOG_FILE = "etl_log.txt";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public static void logMessage(String message) {
        String timestampedMessage = LocalDateTime.now().format(formatter) + ": " + message;
        
        // Log to console via SLF4J
        logger.info(message);
        
        // Also append to file for compatibility with original Python version
        try (FileWriter writer = new FileWriter(LOG_FILE, true)) {
            writer.write(timestampedMessage + "\n");
        } catch (IOException e) {
            logger.error("Failed to write to log file: " + LOG_FILE, e);
        }
    }
    
    public static void logError(String message, Throwable throwable) {
        String timestampedMessage = LocalDateTime.now().format(formatter) + ": ERROR - " + message;
        
        logger.error(message, throwable);
        
        try (FileWriter writer = new FileWriter(LOG_FILE, true)) {
            writer.write(timestampedMessage + "\n");
            if (throwable != null) {
                writer.write(timestampedMessage + " - " + throwable.getMessage() + "\n");
            }
        } catch (IOException e) {
            logger.error("Failed to write error to log file: " + LOG_FILE, e);
        }
    }
}
