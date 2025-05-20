package org.example.payment_guard;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Kafka에서 데이터를 읽어 그대로 출력하고 다른 토픽으로 전달하는 간단한 테스트 프로그램
 */
public class SimpleKafkaTest {

    public static void main(String[] args) throws Exception {
        // 설정 파일 로드
        Properties appProps = new Properties();
        try (InputStream input = SimpleKafkaTest.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                appProps.load(input);
                System.out.println("application.properties 로드 완료");
            } else {
                System.out.println("application.properties 파일을 찾을 수 없습니다. 기본 설정을 사용합니다.");
            }
        } catch (Exception e) {
            System.err.println("application.properties 로드 중 오류: " + e.getMessage());
        }

        // Kafka 설정 가져오기
        String bootstrapServers = appProps.getProperty("bootstrap.servers", "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092");
        String groupId = appProps.getProperty("group.id", "simple_kafka_test");
        String inputTopic = appProps.getProperty("input.topic", "test-topic");
        String outputTopic = appProps.getProperty("output.topic", "3_non_response");

        System.out.println("\n***** Kafka 연결 정보 *****");
        System.out.println("Kafka 브로커 주소: " + bootstrapServers);
        System.out.println("Consumer 그룹 ID: " + groupId);
        System.out.println("입력 토픽: " + inputTopic);
        System.out.println("출력 토픽: " + outputTopic);
        System.out.println("***********************\n");

        // Flink 실행 환경 설정
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka 소스 설정 - 문자열로 직접 처리
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Kafka 싱크 설정 - JSON 그대로 전달
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 데이터 스트림 설정
        DataStream<String> sourceStream = env.fromSource(source, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka String Source");

        // 데이터 처리 및 로깅
        DataStream<String> processedStream = sourceStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) {
                System.out.println("\n=============== 원본 메시지 수신 ===============");
                System.out.println(value);
                System.out.println("==============================================\n");
                
                // JSON 문자열에서 store_id, store_brand, store_name 추출하여 로깅
                try {
                    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                    com.fasterxml.jackson.databind.JsonNode rootNode = mapper.readTree(value);
                    
                    int storeId = rootNode.has("store_id") ? rootNode.get("store_id").asInt() : -1;
                    String storeBrand = rootNode.has("store_brand") ? rootNode.get("store_brand").asText() : "unknown";
                    String storeName = rootNode.has("store_name") ? rootNode.get("store_name").asText() : "unknown";
                    
                    System.out.println("추출된 정보: store_id=" + storeId + 
                                     ", store_brand=" + storeBrand + 
                                     ", store_name=" + storeName);
                } catch (Exception e) {
                    System.err.println("JSON 파싱 오류: " + e.getMessage());
                }
                
                // 그대로 출력 토픽으로 전달
                out.collect(value);
            }
        }).name("Log and Process Raw Messages");

        // 싱크에 연결
        processedStream.sinkTo(sink);

        System.out.println("Simple Kafka Test 실행 준비 완료");
        env.execute("Simple Kafka Test");
    }
}