package org.example.payment_guard.model;

import java.util.Objects;

/**
 * 결제 정보를 담는 모델 클래스입니다.
 */
public class Payment {
    private String transactionId;
    private String userId;
    private String storeId;
    private double amount;
    private long timestamp;

    // Constructors
    public Payment() {
    }

    public Payment(String transactionId, String userId, String storeId, double amount, long timestamp) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.storeId = storeId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payment payment = (Payment) o;
        return Double.compare(payment.amount, amount) == 0 &&
                timestamp == payment.timestamp &&
                Objects.equals(transactionId, payment.transactionId) &&
                Objects.equals(userId, payment.userId) &&
                Objects.equals(storeId, payment.storeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, userId, storeId, amount, timestamp);
    }

    @Override
    public String toString() {
        return "Payment{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", storeId='" + storeId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }
}
