package org.example.payment_guard;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 간단한 카프카 연결 테스트 프로그램
 */
public class SimpleKafkaTest {
    private static final String KAFKA_BOOTSTRAP_SERVERS = "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092";
    private static final String KAFKA_TOPIC = "3_non_response";

    public static void main(String[] args) throws Exception {
        // Flink 실행 환경 설정
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Kafka 프로듀서 속성 설정
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProps.setProperty("transaction.timeout.ms", "30000");
        
        // 간단한 문자열 프로듀서 생성
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                KAFKA_TOPIC,                // 타겟 토픽
                new SimpleStringSchema(),   // 직렬화 스키마
                kafkaProps                  // Kafka 설정
        );
        
        // 테스트 메시지 생성
        DataStream<String> messageStream = env.fromElements(
                "테스트 메시지 1: 현재 시간 " + System.currentTimeMillis(),
                "테스트 메시지 2: 단순 Kafka 연결 테스트"
        );
        
        // 메시지 출력 (디버깅용)
        messageStream.print();
        
        // Kafka로 메시지 전송
        messageStream.addSink(producer);
        
        // 실행
        System.out.println("Flink 작업 시작: Kafka 토픽 " + KAFKA_TOPIC + "로 메시지 전송 시도");
        env.execute("Simple Kafka Test");
    }
}
