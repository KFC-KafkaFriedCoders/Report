package org.example.payment_guard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.example.test1.entity.ReceiptData;
import org.example.payment_guard.deserializer.BinaryReceiptDataDeserializationSchema;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Main extends KeyedProcessFunction<String, ReceiptData, JsonNode> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private transient MapState<String, Long> storeLastActivityState;
    
    // 스토어 상태를 추적하기 위한 내부 클래스 정의
    static class StoreState {
        String brand;
        String name;
        long lastActivityTime;
        
        StoreState(String brand, String name, long lastActivityTime) {
            this.brand = brand;
            this.name = name;
            this.lastActivityTime = lastActivityTime;
        }
    }
    
    // 현재 활성화된 상점 상태 추적
    private Map<Integer, StoreState> activeStores = new ConcurrentHashMap<>();
    
    // 알림 간격 (밀리초)
    private static final long INACTIVITY_THRESHOLD_MS = 5000;
    
    // 알려진 브랜드 목록
    private static final List<String> KNOWN_BRANDS = Arrays.asList(
        "한신포차", "대한국밥", "돌배기집", "롤링파스타", "리춘시장", "막이오름", 
        "미정국수0410", "백스비어", "본가", "빽다방", "빽보이피자", "새마을식당", 
        "역전우동0410", "연돈볼카츠", "원조쌈밥집", "인생설렁탕", "재순식당", 
        "홍콩반점0410", "고투웍"
    );

    @Override
    public void open(Configuration parameters) throws Exception {
        storeLastActivityState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>(
                "storeLastActivity",
                TypeInformation.of(new TypeHint<String>(){}),
                TypeInformation.of(new TypeHint<Long>(){})
            )
        );
    }

    @Override
    public void processElement(ReceiptData value, Context ctx, Collector<JsonNode> out) throws Exception {
        if (value == null) {
            System.out.println("수신된 ReceiptData가 null입니다. 처리를 건너뜁니다.");
            return;
        }
        
        // 상점 ID와 브랜드 정보 가져오기
        int storeId = Math.abs(value.getStoreId()) % 1000; // ID 값 정규화
        String brand = normalizeStoreBrand(value.getStoreBrand());
        String name = normalizeStoreName(value.getStoreName(), storeId, brand);
        
        // 현재 시간
        long now = System.currentTimeMillis();
        
        // 활성 상점 상태 업데이트
        activeStores.put(storeId, new StoreState(brand, name, now));
        
        // 수신된 데이터 로깅
        System.out.println("데이터 수신: store_id=" + storeId + 
                           ", store_brand=" + brand + 
                           ", store_name=" + name);
        
        // 일정 시간 후에 타이머 설정
        if (ctx != null) {
            ctx.timerService().registerProcessingTimeTimer(
                ctx.timerService().currentProcessingTime() + INACTIVITY_THRESHOLD_MS
            );
        }
    }
    
    /**
     * 브랜드명 정규화
     */
    private String normalizeStoreBrand(String brand) {
        if (brand == null || brand.isEmpty()) {
            return getBrandFromIndex((int)(Math.random() * KNOWN_BRANDS.size()));
        }
        
        // 알려진 브랜드 목록에 있는지 확인
        for (String knownBrand : KNOWN_BRANDS) {
            if (brand.contains(knownBrand) || knownBrand.contains(brand)) {
                return knownBrand;
            }
        }
        
        // 알려진 브랜드가 아니면 임의로 선택
        return getBrandFromIndex((int)(Math.random() * KNOWN_BRANDS.size()));
    }
    
    /**
     * 인덱스 기반으로 브랜드 가져오기
     */
    private String getBrandFromIndex(int index) {
        if (index < 0 || index >= KNOWN_BRANDS.size()) {
            return KNOWN_BRANDS.get(0);
        }
        return KNOWN_BRANDS.get(index);
    }
    
    /**
     * 상점명 정규화
     */
    private String normalizeStoreName(String name, int storeId, String brand) {
        if (name != null && !name.isEmpty() && !name.contains("#")) {
            return name;
        }
        
        // 지역명 생성
        String[] locations = {
            "강남", "서초", "잠실", "홍대", "신촌", "종로", "명동", "을지로", 
            "성수", "이태원", "역삼", "대학로", "구의", "연남", "선릉", "안국", "삼성"
        };
        
        int locationIndex = Math.abs((brand.hashCode() + storeId) % locations.length);
        return locations[locationIndex] + "점";
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JsonNode> out) throws Exception {
        long now = System.currentTimeMillis();
        
        // 각 활성 상점의 비활성 상태 확인
        for (Map.Entry<Integer, StoreState> entry : activeStores.entrySet()) {
            int storeId = entry.getKey();
            StoreState state = entry.getValue();
            
            // 비활성 상태 확인
            long inactiveMillis = now - state.lastActivityTime;
            if (inactiveMillis > INACTIVITY_THRESHOLD_MS) {
                // 상점 정보
                String brand = state.brand;
                String name = state.name;
                
                // "알 수 없는 브랜드"나 "알 수 없는 상점"인 경우 알림 생성하지 않음
                if (brand.equals("알 수 없는 브랜드") || name.equals("알 수 없는 상점")) {
                    System.out.println("알 수 없는 브랜드 또는 상점 제외: store_id=" + storeId + 
                                   ", store_brand=" + brand + 
                                   ", store_name=" + name);
                    // 상태에서 제거
                    activeStores.remove(storeId);
                    continue; // 다음 상점으로 넘어감
                }
                
                // 알림 생성
                ObjectNode node = objectMapper.createObjectNode();
                node.put("alert_type", "inactivity");
                node.put("store_id", storeId);
                node.put("store_brand", brand);
                node.put("store_name", name);
                node.put("last_activity_time", state.lastActivityTime);
                node.put("current_time", now);
                node.put("inactive_seconds", inactiveMillis / 1000);
                
                System.out.println("알림 생성: store_id=" + storeId + 
                               ", store_brand=" + brand + 
                               ", store_name=" + name);
                
                out.collect(node);
                
                // 알림 생성 후 상태에서 제거 (다시 활동이 감지될 때까지)
                activeStores.remove(storeId);
            }
        }
        
        // 다음 타이머 설정
        ctx.timerService().registerProcessingTimeTimer(
            ctx.timerService().currentProcessingTime() + INACTIVITY_THRESHOLD_MS
        );
    }

    public static class JsonSerializationSchema implements SerializationSchema<JsonNode> {
        @Override
        public byte[] serialize(JsonNode element) {
            try {
                // UTF-8 인코딩 명시적 지정
                return objectMapper.writeValueAsString(element).getBytes("UTF-8");
            } catch (Exception e) {
                throw new RuntimeException("JSON serialization error", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 설정 파일 로드
        Properties appProps = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                appProps.load(input);
                System.out.println("application.properties 로드 완료");
            } else {
                System.out.println("application.properties 파일을 찾을 수 없습니다. 기본 설정을 사용합니다.");
            }
        } catch (Exception e) {
            System.err.println("application.properties 로드 중 오류: " + e.getMessage());
        }
        
        // Flink 설정 최적화
        Configuration config = new Configuration();
        // TaskManager 메모리 설정
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.ofMebiBytes(384));
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(128));
        config.setString("parallelism.default", "1");
        config.setString("state.backend", "hashmap");
        config.setString("state.checkpoint-storage", "jobmanager");
        
        // 최적화된 설정으로 Flink 환경 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0); // Watermark 간격 비활성화
        
        // 메모리 사용량 최적화를 위한 체크포인팅 사용
        env.disableOperatorChaining(); // 연산자 체이닝 비활성화로 메모리 압력 감소
        
        // 소스 및 싱크 구성
        KafkaSource<ReceiptData> source = KafkaSource.<ReceiptData>builder()
            .setBootstrapServers("localhost:9094")
            .setTopics("test-topic")
            .setGroupId("store_inactivity_detector")
            .setValueOnlyDeserializer(new BinaryReceiptDataDeserializationSchema())
            .build();

        KafkaSink<JsonNode> sink = KafkaSink.<JsonNode>builder()
            .setBootstrapServers("localhost:9094")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("3_non_response")
                    .setValueSerializationSchema(new JsonSerializationSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        // 데이터 스트림 설정
        DataStream<ReceiptData> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<JsonNode> alertStream = sourceStream
            .keyBy(data -> "global-key")
            .process(new Main())
            .filter(node -> {
                // "알 수 없는 브랜드"나 "알 수 없는 상점"인 메시지 필터링
                String brand = node.get("store_brand").asText();
                String name = node.get("store_name").asText();
                boolean isValid = !brand.equals("알 수 없는 브랜드") && !name.equals("알 수 없는 상점");
                
                if (!isValid) {
                    System.out.println("필터링됨: 알 수 없는 브랜드 또는 상점 - " + node.toString());
                }
                
                return isValid;
            });

        alertStream.sinkTo(sink);
        
        System.out.println("Store Inactivity Detector 실행 준비 완료");
        env.execute("Store Inactivity Detector");
    }
}