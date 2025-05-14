package org.example.payment_guard.model;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 알람 정보를 담는 모델 클래스입니다.
 */
public class Alert {
    private String storeId;
    private String alertType;
    private String message;
    private long timestamp;

    // Constructors
    public Alert() {
    }

    public Alert(String storeId, String alertType, String message, long timestamp) {
        this.storeId = storeId;
        this.alertType = alertType;
        this.message = message;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp), 
                ZoneId.systemDefault()
        );
        String formattedTime = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        
        return "🚨 ALERT 🚨 [" + formattedTime + "] - " + alertType + ": " + message;
    }
}
