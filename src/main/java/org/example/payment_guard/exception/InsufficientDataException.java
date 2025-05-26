package org.example.payment_guard.exception;

public class InsufficientDataException extends RuntimeException {
    private final int requestedCount;
    private final int actualCount;

    public InsufficientDataException(int requestedCount, int actualCount) {
        super(String.format("데이터가 부족합니다! (요청: %d건, 실제: %d건)", requestedCount, actualCount));
        this.requestedCount = requestedCount;
        this.actualCount = actualCount;
    }

    public int getRequestedCount() {
        return requestedCount;
    }

    public int getActualCount() {
        return actualCount;
    }
}
