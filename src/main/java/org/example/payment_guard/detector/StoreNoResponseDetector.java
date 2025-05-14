package org.example.payment_guard.detector;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.payment_guard.model.Alert;
import org.example.payment_guard.model.Payment;

/**
 * 매장 미응답을 탐지하는 클래스입니다.
 * 특정 매장에서 30초 동안 결제가 없으면 알람을 발생시킵니다.
 */
public class StoreNoResponseDetector extends KeyedProcessFunction<String, Payment, Alert> {

    private static final long RESPONSE_TIMEOUT_MS = 30 * 1000; // 30초
    private MapState<String, Long> lastPaymentTimestampMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                "last-payment-timestamp",
                TypeInformation.of(new TypeHint<String>() {}),
                TypeInformation.of(new TypeHint<Long>() {})
        );
        lastPaymentTimestampMap = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(Payment payment, Context ctx, Collector<Alert> out) throws Exception {
        String storeId = payment.getStoreId();
        long currentTime = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        
        // 매장의 마지막 결제 시간 업데이트
        lastPaymentTimestampMap.put(storeId, currentTime);
        
        // 타이머 등록 (30초 후에 체크)
        ctx.timerService().registerProcessingTimeTimer(currentTime + RESPONSE_TIMEOUT_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        String storeId = ctx.getCurrentKey();
        
        // 마지막 결제 시간 확인
        if (lastPaymentTimestampMap.contains(storeId)) {
            long lastPaymentTime = lastPaymentTimestampMap.get(storeId);
            
            // 30초 이상 경과했으면 알람 발생
            if (timestamp >= lastPaymentTime + RESPONSE_TIMEOUT_MS) {
                String message = "매장 ID " + storeId + "에서 30초 동안 결제가 없습니다!";
                Alert alert = new Alert(
                        storeId,
                        "STORE_NO_RESPONSE",
                        message,
                        timestamp
                );
                out.collect(alert);
            }
        }
    }
}
