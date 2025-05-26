package org.example.payment_guard.service;

import com.example.test1.entity.ReceiptData;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.example.payment_guard.SampleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private static final String BOOTSTRAP = "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092";
    private static final String SCHEMA_REGISTRY = "http://43.201.175.172:8081,http://43.202.127.159:8081";
    private static final String TOPIC = "test-topic";
    private static final String GROUP_ID = "kafka-pg-ingestion-" + UUID.randomUUID();

    private static final int BATCH_SIZE = 10;
    private static final DateTimeFormatter INPUT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter ISO_FMT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPass;
    
    private ExecutorService executorService;
    private volatile boolean running = false;

    public KafkaConsumerService() {
        Dotenv env = Dotenv.configure().ignoreIfMissing().load();
        this.jdbcUrl = env.get("DB_URL");
        this.jdbcUser = env.get("DB_USER");
        this.jdbcPass = env.get("DB_PASSWORD");
        
        if (jdbcUrl == null || jdbcUser == null || jdbcPass == null) {
            throw new IllegalStateException("DB 환경변수가 설정되지 않았습니다. .env 파일을 확인해주세요.");
        }
    }

    @PostConstruct
    public void startKafkaConsumer() {
        logger.info("🚀 Kafka Consumer 서비스 시작...");
        
        executorService = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "kafka-consumer-thread");
            t.setDaemon(true);
            return t;
        });
        
        running = true;
        executorService.submit(this::consumeMessages);
    }

    @PreDestroy
    public void stopKafkaConsumer() {
        logger.info("🛑 Kafka Consumer 서비스 종료...");
        running = false;
        
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private void consumeMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY);
        props.put("specific.avro.reader", true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<byte[], ReceiptData> consumer = new KafkaConsumer<>(props);
             Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) {

            conn.setAutoCommit(false);

            consumer.subscribe(Collections.singleton(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // nothing
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    consumer.seekToEnd(partitions);
                    partitions.forEach(tp -> {
                        long offset = consumer.position(tp);
                        logger.info("🔄 Starting at end offset {} for {}", offset, tp);
                    });
                }
            });

            logger.info("⏳ Listening for records on topic '{}'...", TOPIC);

            String sql = "INSERT INTO receipt_raw (" +
                        "franchise_id, store_brand, store_id, store_name, region, store_address, " +
                        "menu_items, total_price, user_id, event_time, user_name, user_gender, user_age" +
                        ") VALUES (?,?,?,?,?,?, ?::jsonb ,?,?,?,?,?,?)";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int buffer = 0;

                while (running) {
                    ConsumerRecords<byte[], ReceiptData> records = consumer.poll(java.time.Duration.ofSeconds(1));
                    
                    for (var rec : records) {
                        ReceiptData r = rec.value();
                        setParams(ps, r);
                        ps.addBatch();
                        buffer++;

                        if (buffer >= BATCH_SIZE) {
                            flushBatch(ps, conn, consumer);
                            buffer = 0;
                        }
                    }
                }
            }

        } catch (Exception e) {
            if (running) {
                logger.error("Kafka Consumer 실행 중 오류 발생", e);
            } else {
                logger.info("Kafka Consumer 정상 종료");
            }
        }
    }

    private void setParams(PreparedStatement ps, ReceiptData r) throws SQLException {
        ps.setInt(1, r.getFranchiseId());
        ps.setString(2, r.getStoreBrand());
        ps.setInt(3, r.getStoreId());
        ps.setString(4, r.getStoreName());
        ps.setString(5, r.getRegion());
        ps.setString(6, r.getStoreAddress());
        ps.setString(7, SampleUtils.menuItemsToJson(r.getMenuItems()));
        ps.setInt(8, r.getTotalPrice());
        ps.setInt(9, r.getUserId());
        ps.setTimestamp(10, Timestamp.from(parseToInstant(r.getTime())));
        ps.setString(11, r.getUserName());
        ps.setString(12, r.getUserGender());
        ps.setInt(13, r.getUserAge());
        
        logger.debug("📦 Inserting record: {} | {} | {}₩", 
                    r.getStoreBrand(), r.getStoreName(), r.getTotalPrice());
    }

    private void flushBatch(PreparedStatement ps, Connection conn, KafkaConsumer<?, ?> consumer) {
        try {
            ps.executeBatch();
            logger.debug("📝 Batch executed. Committing transaction...");
            conn.commit();
            consumer.commitSync();
            logger.info("✅ Committed {} rows → PostgreSQL", BATCH_SIZE);

        } catch (SQLException e) {
            try { 
                conn.rollback(); 
            } catch (SQLException ignore) {}
            logger.error("배치 처리 중 오류 발생", e);
        }
    }

    private Instant parseToInstant(String timeStr) {
        try {
            return LocalDateTime.parse(timeStr, INPUT_FMT)
                    .atOffset(ZoneOffset.UTC)
                    .toInstant();
        } catch (DateTimeParseException e) {
            return OffsetDateTime.parse(timeStr, ISO_FMT).toInstant();
        }
    }
}
