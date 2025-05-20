package org.example.payment_guard;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Kafka에서 Avro 바이너리 데이터를 읽어 처리하는 클래스
 */
public class RawDataProcessor {

    // Avro 스키마 정의 - purchase-main의 ReceiptData.avsc와 동일한 스키마 정의
    private static final String RECEIPT_DATA_SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"ReceiptData\","
        + "\"namespace\":\"com.example.test1.entity\","
        + "\"fields\":["
        + "{\"name\":\"franchise_id\",\"type\":\"int\"},"
        + "{\"name\":\"store_brand\",\"type\":\"string\"},"
        + "{\"name\":\"store_id\",\"type\":\"int\"},"
        + "{\"name\":\"store_name\",\"type\":\"string\"},"
        + "{\"name\":\"region\",\"type\":\"string\"},"
        + "{\"name\":\"store_address\",\"type\":\"string\"},"
        + "{\"name\":\"menu_items\",\"type\":{"
        + "  \"type\":\"array\","
        + "  \"items\":{"
        + "    \"type\":\"record\","
        + "    \"name\":\"MenuItem\","
        + "    \"fields\":["
        + "      {\"name\":\"menu_id\",\"type\":\"int\"},"
        + "      {\"name\":\"menu_name\",\"type\":\"string\"},"
        + "      {\"name\":\"unit_price\",\"type\":\"int\"},"
        + "      {\"name\":\"quantity\",\"type\":\"int\"}"
        + "    ]"
        + "  }"
        + "}},"
        + "{\"name\":\"total_price\",\"type\":\"int\"},"
        + "{\"name\":\"user_id\",\"type\":\"int\"},"
        + "{\"name\":\"time\",\"type\":\"string\"},"
        + "{\"name\":\"user_name\",\"type\":\"string\"},"
        + "{\"name\":\"user_gender\",\"type\":\"string\"},"
        + "{\"name\":\"user_age\",\"type\":\"int\"}"
        + "]"
        + "}";

    public static void main(String[] args) throws Exception {
        // 설정 파일 로드
        Properties appProps = new Properties();
        try (InputStream input = RawDataProcessor.class.getClassLoader().getResourceAsStream("application.properties")) {
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
        String groupId = appProps.getProperty("group.id", "raw_data_processor");
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

        // Kafka 소스 속성 설정
        Properties sourceProps = new Properties();
        sourceProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        sourceProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        sourceProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        sourceProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        sourceProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        sourceProps.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        sourceProps.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");

        // 바이트 배열을 직접 처리하는 커스텀 ByteArrayDeserializationSchema 클래스 정의
        class ByteArrayDeserializationSchema implements org.apache.flink.api.common.serialization.DeserializationSchema<byte[]> {
            @Override
            public byte[] deserialize(byte[] message) {
                return message;
            }

            @Override
            public boolean isEndOfStream(byte[] nextElement) {
                return false;
            }

            @Override
            public org.apache.flink.api.common.typeinfo.TypeInformation<byte[]> getProducedType() {
                return org.apache.flink.api.common.typeinfo.TypeInformation.of(byte[].class);
            }
        }

        // Kafka 소스 설정 - 바이트 배열로 직접 처리
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
                .setProperties(sourceProps)
                .build();

        // Kafka 싱크 설정 - JSON 문자열로 변환하여 전달
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
        DataStream<byte[]> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Byte Source");

        // 데이터 처리 및 로깅
        DataStream<String> processedStream = sourceStream.process(new AvroProcessFunction())
                .name("Process Raw Data");

        // 싱크에 연결
        processedStream.sinkTo(sink);

        System.out.println("Raw Data Processor 실행 준비 완료");
        env.execute("Raw Data Processor");
    }
    
    /**
     * 바이트 배열을 16진수 문자열로 변환
     */
    private static String bytesToHex(byte[] bytes, int offset, int length) {
        StringBuilder result = new StringBuilder();
        for (int i = offset; i < offset + length; i++) {
            result.append(String.format("%02X ", bytes[i]));
        }
        return result.toString();
    }
    
    /**
     * Avro 처리를 위한 직렬화 가능한 ProcessFunction 클래스
     */
    public static class AvroProcessFunction extends ProcessFunction<byte[], String> implements Serializable {
        private static final long serialVersionUID = 1L;
        private transient Schema schema;
        private transient DatumReader<GenericRecord> reader;
        private transient BinaryDecoder decoder;
        
        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            // open 메서드에서 스키마와 reader 초기화 (직렬화 문제 해결)
            schema = new Schema.Parser().parse(RECEIPT_DATA_SCHEMA_JSON);
            reader = new GenericDatumReader<>(schema);
        }
        
        @Override
        public void processElement(byte[] value, Context ctx, Collector<String> out) throws Exception {
            // 원본 바이트 배열 정보 출력
            System.out.println("\n----- 수신된 바이트 데이터 정보 -----");
            System.out.println("Data length: " + (value != null ? value.length : "null"));
            
            // 바이트 데이터에서 직접 프랜차이즈 ID와 스토어 ID 추출
            int franchiseId = 0;
            int storeId = 0;
            
            if (value != null && value.length > 6) { // 헤더 4바이트 + 프랜차이즈 ID 1바이트 + 스토어 ID 1바이트
                System.out.println("First 20 bytes (hex): " + bytesToHex(value, 0, Math.min(20, value.length)));
                
                // 첫 4바이트는 헤더나 마커로 간주
                franchiseId = value[4] & 0xFF; // 바이트를 부호 없는 정수로 변환 (0-255)
                storeId = value[5] & 0xFF;
                
                System.out.println("바이트 데이터에서 직접 추출: Franchise ID = " + franchiseId + ", Store ID = " + storeId);
                
                try {
                    // Avro 역직렬화 적용
                    GenericRecord record = decodeAvroData(value);
                    
                    if (record != null) {
                        // Avro 레코드에서 필드 추출하여 출력
                        System.out.println("\n=============== 올바르게 역직렬화된 데이터 ===============");
                        printReceiptData(record);
                        System.out.println("==============================================\n");
                        
                        // 현재는 직접 추출한 ID 사용
                        String jsonOutput = createJsonOutput(franchiseId, storeId, record);
                        out.collect(jsonOutput);
                        return;
                    }
                } catch (Exception e) {
                    System.err.println("Avro 역직렬화 실패: " + e.getMessage());
                    // 역직렬화 실패 시 백업 방법 시도
                    try {
                        // 첫 4바이트 제거 후 시도
                        int offset = 4; // Avro 파일 헤더 패턴
                        if (value.length > offset) {
                            byte[] processedBytes = new byte[value.length - offset];
                            System.arraycopy(value, offset, processedBytes, 0, processedBytes.length);
                            GenericRecord record = decodeAvroData(processedBytes);
                            
                            if (record != null) {
                                System.out.println("\n헤더 제거 후 역직렬화 성공!");
                                printReceiptData(record);
                                String jsonOutput = createJsonOutput(franchiseId, storeId, record);
                                out.collect(jsonOutput);
                                return;
                            }
                        }
                    } catch (Exception e2) {
                        System.err.println("백업 역직렬화 방법도 실패: " + e2.getMessage());
                    }
                }
                
                // 모든 Avro 역직렬화 시도 실패 시, 원본 데이터를 UTF-8로 출력해보기
                try {
                    String rawString = new String(value, StandardCharsets.UTF_8);
                    System.out.println("\n=============== 원본 UTF-8 변환 데이터 ===============");
                    System.out.println(rawString);
                    System.out.println("==============================================\n");
                    
                    // UTF-8 텍스트에서 상점 브랜드와 이름 추출 시도
                    String storeBrand = extractBrandFromText(rawString);
                    String storeName = extractBranchNameFromText(rawString);
                    
                    // 사용자 정보 추출
                    String jsonOutput = createSimpleJsonOutput(franchiseId, storeId, storeBrand, storeName);
                    out.collect(jsonOutput);
                    return;
                } catch (Exception e) {
                    System.err.println("UTF-8 변환 실패: " + e.getMessage());
                }
            }
            
            // 데이터 처리 불가능한 경우 기본 JSON 응답 (직접 추출한 ID 사용)
            String defaultJson = "{\"alert_type\":\"inactivity\",\"franchise_id\":" + franchiseId + 
                                ",\"store_id\":" + storeId + 
                                ",\"store_brand\":\"오류 발생\",\"store_name\":\"오류 발생 상점\",\"inactive_seconds\":10}";
            out.collect(defaultJson);
        }
        
        /**
         * Avro 바이너리 데이터 역직렬화
         */
        private GenericRecord decodeAvroData(byte[] data) throws IOException {
            try {
                ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
                decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
                return reader.read(null, decoder);
            } catch (IOException e) {
                System.err.println("Avro 디코딩 오류: " + e.getMessage());
                throw e;
            }
        }
        
        /**
         * ReceiptData 레코드 내용 출력
         */
        private void printReceiptData(GenericRecord record) {
            try {
                // 기본 정보 출력
                System.out.println("프랜차이즈 ID: " + record.get("franchise_id"));
                System.out.println("스토어 브랜드: " + record.get("store_brand"));
                System.out.println("스토어 ID: " + record.get("store_id"));
                System.out.println("스토어 이름: " + record.get("store_name"));
                System.out.println("지역: " + record.get("region"));
                System.out.println("주소: " + record.get("store_address"));
                
                // 메뉴 아이템 출력
                Object menuItemsObj = record.get("menu_items");
                if (menuItemsObj instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<GenericRecord> menuItems = (List<GenericRecord>) menuItemsObj;
                    System.out.println("\n===== 메뉴 항목 =====\n");
                    
                    for (int i = 0; i < menuItems.size(); i++) {
                        GenericRecord menuItem = menuItems.get(i);
                        System.out.println("메뉴 #" + (i+1));
                        System.out.println("  ID: " + menuItem.get("menu_id"));
                        System.out.println("  이름: " + menuItem.get("menu_name"));
                        System.out.println("  단가: " + menuItem.get("unit_price"));
                        System.out.println("  수량: " + menuItem.get("quantity"));
                        System.out.println();
                    }
                }
                
                // 나머지 정보 출력
                System.out.println("총 가격: " + record.get("total_price"));
                System.out.println("사용자 ID: " + record.get("user_id"));
                System.out.println("시간: " + record.get("time"));
                System.out.println("사용자 이름: " + record.get("user_name"));
                System.out.println("사용자 성별: " + record.get("user_gender"));
                System.out.println("사용자 나이: " + record.get("user_age"));
            } catch (Exception e) {
                System.err.println("Record 정보 출력 중 오류: " + e.getMessage());
            }
        }
        
        /**
         * Avro 레코드와 직접 추출한 ID를 사용하여 JSON 문자열 생성
         */
        private String createJsonOutput(int franchiseId, int storeId, GenericRecord record) {
            try {
                // 필요한 필드 추출
                String storeBrand = "알 수 없음";
                String storeName = "알 수 없음";
                
                // 기본 정보 추출
                if (record.get("store_brand") != null) {
                    storeBrand = record.get("store_brand").toString();
                }
                
                if (record.get("store_name") != null) {
                    storeName = record.get("store_name").toString();
                }
                
                // 타임스탬프 및 포맷터 청정
                long lastActivityTime = System.currentTimeMillis() - 11000;
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String lastActivityTimeStr = dateFormat.format(new Date(lastActivityTime));
                String currentTimeStr = dateFormat.format(new Date(currentTime));
                
                // 터미널에 진행 상황 출력
                System.out.println("\n*****************************************");
                System.out.println("비활성 상점 감지 중:");
                System.out.println("franchise_id: " + franchiseId);
                System.out.println("store_id: " + storeId);
                System.out.println("store_brand: " + storeBrand);
                System.out.println("store_name: " + storeName);
                System.out.println("last_activity_time: " + lastActivityTimeStr);
                System.out.println("current_time: " + currentTimeStr);
                System.out.println("inactive_seconds: 11");
                System.out.println("*****************************************\n");
                
                // JSON 구성
                StringBuilder json = new StringBuilder();
                json.append("{");
                json.append("\"alert_type\":\"inactivity\",");
                json.append("\"franchise_id\":").append(franchiseId).append(",");
                json.append("\"store_id\":").append(storeId).append(",");
                json.append("\"store_brand\":\"").append(escapeJson(storeBrand)).append("\",");
                json.append("\"store_name\":\"").append(escapeJson(storeName)).append("\",");
                // json.append("\"last_activity_time\":").append(lastActivityTime).append(",");
                json.append("\"last_activity_time\":\"").append(lastActivityTimeStr).append("\",");
                // json.append("\"current_time\":").append(currentTime).append(",");
                json.append("\"current_time\":\"").append(currentTimeStr).append("\",");
                json.append("\"inactive_seconds\":11");
                json.append("}");
                
                return json.toString();
            } catch (Exception e) {
                System.err.println("JSON 생성 중 오류: " + e.getMessage());
                // 타임스탬프 및 포맷터 청정
                long lastActivityTime = System.currentTimeMillis() - 10000;
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String lastActivityTimeStr = dateFormat.format(new Date(lastActivityTime));
                String currentTimeStr = dateFormat.format(new Date(currentTime));
                
                // 터미널에 오류 상황 출력
                System.out.println("\n*****************************************");
                System.out.println("오류 발생 - 기본 알림 생성:");
                System.out.println("franchise_id: " + franchiseId);
                System.out.println("store_id: " + storeId);
                System.out.println("store_brand: 오류 발생");
                System.out.println("store_name: 오류 발생 상점");
                System.out.println("last_activity_time: " + lastActivityTimeStr);
                System.out.println("current_time: " + currentTimeStr);
                System.out.println("inactive_seconds: 10");
                System.out.println("*****************************************\n");
                
                return "{\"alert_type\":\"inactivity\",\"franchise_id\":" + franchiseId + 
                      ",\"store_id\":" + storeId + 
                      ",\"store_brand\":\"오류 발생\",\"store_name\":\"오류 발생 상점\"" + 
                      ",\"last_activity_time\":\"" + lastActivityTimeStr + "\"" + 
                      ",\"current_time\":\"" + currentTimeStr + "\"" + 
                      ",\"inactive_seconds\":10}";
            }
        }
        
        /**
         * 텍스트에서 상점 이름과 브랜드를 추출하여 JSON 생성
         */
        private String createSimpleJsonOutput(int franchiseId, int storeId, String storeBrand, String storeName) {
            try {
                // 타임스탬프 및 포맷터 청정
                long lastActivityTime = System.currentTimeMillis() - 11000;
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String lastActivityTimeStr = dateFormat.format(new Date(lastActivityTime));
                String currentTimeStr = dateFormat.format(new Date(currentTime));
                
                // 터미널에 진행 상황 출력
                System.out.println("\n*****************************************");
                System.out.println("패턴 기반 비활성 상점 감지:");
                System.out.println("franchise_id: " + franchiseId);
                System.out.println("store_id: " + storeId);
                System.out.println("store_brand: " + storeBrand);
                System.out.println("store_name: " + storeName);
                System.out.println("last_activity_time: " + lastActivityTimeStr);
                System.out.println("current_time: " + currentTimeStr);
                System.out.println("inactive_seconds: 11");
                System.out.println("*****************************************\n");
                
                // JSON 구성
                StringBuilder json = new StringBuilder();
                json.append("{");
                json.append("\"alert_type\":\"inactivity\",");
                json.append("\"franchise_id\":").append(franchiseId).append(",");
                json.append("\"store_id\":").append(storeId).append(",");
                json.append("\"store_brand\":\"").append(escapeJson(storeBrand)).append("\",");
                json.append("\"store_name\":\"").append(escapeJson(storeName)).append("\",");
                // json.append("\"last_activity_time\":").append(lastActivityTime).append(",");
                json.append("\"last_activity_time\":\"").append(lastActivityTimeStr).append("\",");
                // json.append("\"current_time\":").append(currentTime).append(",");
                json.append("\"current_time\":\"").append(currentTimeStr).append("\",");
                json.append("\"inactive_seconds\":11");
                json.append("}");
                
                return json.toString();
            } catch (Exception e) {
                System.err.println("간단한 JSON 생성 중 오류: " + e.getMessage());
                // 타임스탬프 및 포맷터 청정
                long lastActivityTime = System.currentTimeMillis() - 10000;
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String lastActivityTimeStr = dateFormat.format(new Date(lastActivityTime));
                String currentTimeStr = dateFormat.format(new Date(currentTime));
                
                return "{\"alert_type\":\"inactivity\",\"franchise_id\":" + franchiseId + 
                      ",\"store_id\":" + storeId + 
                      ",\"store_brand\":\"오류 발생\",\"store_name\":\"오류 발생 상점\"" + 
                      ",\"last_activity_time\":\"" + lastActivityTimeStr + "\"" + 
                      ",\"current_time\":\"" + currentTimeStr + "\"" + 
                      ",\"inactive_seconds\":10}";
            }
        }
        
        /**
         * UTF-8 텍스트에서 브랜드명 추출
         */
        private String extractBrandFromText(String text) {
            if (text == null || text.isEmpty()) {
                return "알 수 없음";
            }
            
            // 한글로 시작하는 첫 단어가 브랜드명일 가능성이 높음
            int start = 0;
            while (start < text.length() && !isKorean(text.charAt(start))) {
                start++;
            }
            
            if (start >= text.length()) {
                return "알 수 없음";
            }
            
            // 브랜드명의 끝 찾기 (다음 공백 또는 숫자까지)
            int end = start;
            while (end < text.length() && 
                   (isKorean(text.charAt(end)) || 
                    Character.isLetter(text.charAt(end)) || 
                    Character.isDigit(text.charAt(end)))) {
                end++;
            }
            
            return text.substring(start, end).trim();
        }
        
        /**
         * UTF-8 텍스트에서 지점명 추출
         */
        private String extractBranchNameFromText(String text) {
            if (text == null || text.isEmpty()) {
                return "알 수 없음점";
            }
            
            // "XX점" 패턴 찾기
            Pattern pattern = Pattern.compile("([가-힣a-zA-Z0-9\\s]+점)");
            Matcher matcher = pattern.matcher(text);
            
            if (matcher.find()) {
                return matcher.group(1).trim();
            }
            
            // 지역명 + 점 패턴 찾기
            String[] regions = {
                "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", 
                "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"
            };
            
            for (String region : regions) {
                int idx = text.indexOf(region);
                if (idx >= 0) {
                    // 지역명 다음 단어 추출
                    int start = idx + region.length();
                    while (start < text.length() && !isKorean(text.charAt(start)) && !Character.isLetterOrDigit(text.charAt(start))) {
                        start++;
                    }
                    
                    if (start < text.length()) {
                        int end = start;
                        while (end < text.length() && 
                              (isKorean(text.charAt(end)) || 
                               Character.isLetterOrDigit(text.charAt(end)) || 
                               text.charAt(end) == ' ')) {
                            end++;
                        }
                        
                        if (end > start) {
                            String areaName = text.substring(start, end).trim();
                            return areaName + "점";
                        }
                    }
                }
            }
            
            // 아무것도 찾지 못한 경우 기본값 반환
            return "본점";
        }
        
        /**
         * 한글 문자 확인
         */
        private boolean isKorean(char ch) {
            return (ch >= '가' && ch <= '힣') || (ch >= 'ㄱ' && ch <= 'ㅎ');
        }
        
        /**
         * JSON 문자열 이스케이핑
         */
        private String escapeJson(String text) {
            if (text == null) return "";
            return text.replace("\\", "\\\\")
                       .replace("\"", "\\\"")
                       .replace("\b", "\\b")
                       .replace("\f", "\\f")
                       .replace("\n", "\\n")
                       .replace("\r", "\\r")
                       .replace("\t", "\\t");
        }
    }
}