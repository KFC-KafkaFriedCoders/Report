package org.example.payment_guard.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportResponse {
    private boolean success;
    private String message;
    private Integer requestedCount;
    private Integer actualDataCount;
    private String report;
    private long processingTimeMs;

    // 성공 응답 생성자
    public ReportResponse(boolean success, int requestedCount, String report, long processingTimeMs) {
        this.success = success;
        this.requestedCount = requestedCount;
        this.report = report;
        this.processingTimeMs = processingTimeMs;
    }

    // 실패 응답 생성자
    public ReportResponse(boolean success, String message, Integer requestedCount, Integer actualDataCount) {
        this.success = success;
        this.message = message;
        this.requestedCount = requestedCount;
        this.actualDataCount = actualDataCount;
    }

    // 에러 응답 생성자
    public ReportResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    // Getters and Setters
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getRequestedCount() {
        return requestedCount;
    }

    public void setRequestedCount(Integer requestedCount) {
        this.requestedCount = requestedCount;
    }

    public Integer getActualDataCount() {
        return actualDataCount;
    }

    public void setActualDataCount(Integer actualDataCount) {
        this.actualDataCount = actualDataCount;
    }

    public String getReport() {
        return report;
    }

    public void setReport(String report) {
        this.report = report;
    }

    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }
}
