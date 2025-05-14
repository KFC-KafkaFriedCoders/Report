package org.example.payment_guard.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * 테스트용으로 Kafka에 결제 데이터를 생성하는 유틸리티 클래스입니다.
 * 이 클래스는 독립적으로 실행 가능합니다.
 */
public class PaymentDataGenerator {
    private static final String BOOTSTRAP_SERVERS = "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092"; // Broker1,2,3 서버 주소
    private static final String TOPIC = "test-topic";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();

    private static final String[] STORE_IDS = {"store-001", "store-002", "store-003", "store-004", "store-005"};
    private static final String[] USER_IDS = {"user-001", "user-002", "user-003", "user-004", "user-005"};
    private static final double MIN_AMOUNT = 1000.0;
    private static final double MAX_AMOUNT = 50000.0;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("결제 데이터 생성 시작. 종료하려면 Ctrl+C를 누르세요.");

        try {
            while (true) {
                String storeId = STORE_IDS[random.nextInt(STORE_IDS.length)];
                String userId = USER_IDS[random.nextInt(USER_IDS.length)];
                double amount = MIN_AMOUNT + (MAX_AMOUNT - MIN_AMOUNT) * random.nextDouble();
                
                ObjectNode payment = objectMapper.createObjectNode()
                        .put("transactionId", UUID.randomUUID().toString())
                        .put("userId", userId)
                        .put("storeId", storeId)
                        .put("amount", Math.round(amount * 100.0) / 100.0)
                        .put("timestamp", System.currentTimeMillis());
                
                String paymentJson = objectMapper.writeValueAsString(payment);
                
                producer.send(new ProducerRecord<>(TOPIC, storeId, paymentJson));
                
                System.out.println("결제 데이터 생성: " + paymentJson);
                
                // 매장마다 다른 간격으로 데이터 생성 (테스트를 위해)
                if (storeId.equals("store-003")) {
                    // store-003에 대해서는 데이터를 적게 생성하여 알람 테스트
                    Thread.sleep(45000); // 45초 간격
                } else {
                    Thread.sleep(random.nextInt(5000) + 3000); // 3~8초 간격
                }
            }
        } finally {
            producer.close();
        }
    }
}
