package org.example.payment_guard;

import java.time.LocalDateTime;

/**
 * POJO for holding the summarized report result.
 */
public class ReportResult {
    private int franchiseId;
    private int storeId;
    private LocalDateTime analysisTime;
    private int totalSales;
    private String topMenu;
    private double anomalyScore;
    private String summaryText;
    private String fullResultJson;

    public int getFranchiseId() {
        return franchiseId;
    }
    public void setFranchiseId(int franchiseId) {
        this.franchiseId = franchiseId;
    }

    public int getStoreId() {
        return storeId;
    }
    public void setStoreId(int storeId) {
        this.storeId = storeId;
    }

    public LocalDateTime getAnalysisTime() {
        return analysisTime;
    }
    public void setAnalysisTime(LocalDateTime analysisTime) {
        this.analysisTime = analysisTime;
    }

    public int getTotalSales() {
        return totalSales;
    }
    public void setTotalSales(int totalSales) {
        this.totalSales = totalSales;
    }

    public String getTopMenu() {
        return topMenu;
    }
    public void setTopMenu(String topMenu) {
        this.topMenu = topMenu;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }
    public void setAnomalyScore(double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }

    public String getSummaryText() {
        return summaryText;
    }
    public void setSummaryText(String summaryText) {
        this.summaryText = summaryText;
    }

    public String getFullResultJson() {
        return fullResultJson;
    }
    public void setFullResultJson(String fullResultJson) {
        this.fullResultJson = fullResultJson;
    }
}