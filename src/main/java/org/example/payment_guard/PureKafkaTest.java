package org.example.payment_guard;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 순수 Kafka 클라이언트를 사용한 연결 테스트
 */
public class PureKafkaTest {
    private static final String BOOTSTRAP_SERVERS = "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092";
    private static final String TOPIC = "3_non_response";

    public static void main(String[] args) {
        // Kafka 프로듀서 설정
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        System.out.println("Kafka 프로듀서 설정: " + props);
        System.out.println("대상 토픽: " + TOPIC);

        // Kafka 프로듀서 생성
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // 테스트 메시지 생성 및 전송
            String message = "순수 Kafka 클라이언트 테스트 메시지 - " + System.currentTimeMillis();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
            
            System.out.println("메시지 전송 시도: " + message);
            
            // 동기식으로 메시지 전송 (즉시 결과 확인)
            try {
                producer.send(record).get();
                System.out.println("메시지 전송 성공!");
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("메시지 전송 실패: " + e.getMessage());
                e.printStackTrace();
            }
            
            // 추가 메시지 전송
            for (int i = 0; i < 3; i++) {
                String additionalMsg = "추가 테스트 메시지 #" + (i+1) + " - " + System.currentTimeMillis();
                ProducerRecord<String, String> additionalRecord = new ProducerRecord<>(TOPIC, additionalMsg);
                
                System.out.println("추가 메시지 전송 시도: " + additionalMsg);
                
                try {
                    producer.send(additionalRecord).get();
                    System.out.println("추가 메시지 #" + (i+1) + " 전송 성공!");
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("추가 메시지 #" + (i+1) + " 전송 실패: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.out.println("모든 테스트 메시지 전송 시도 완료");
        }
    }
}
