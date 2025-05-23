package org.example.payment_guard;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.example.test1.entity.ReceiptData;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import org.example.payment_guard.GPTReporter;

/**
 * Stand-alone ingestion app: Kafka ➜ PostgreSQL
 * -------------------------------------------------
 * 1. Consumes ReceiptData messages from "test-topic" (Avro-encoded, Confluent Schema Registry)
 * 2. Converts the record into a flat row & writes to receipt_raw table in PostgreSQL
 *
 *  – No Flink dependency –
 */
public class Main {

    private static final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    // ---------- Kafka settings ----------
    private static final String BOOTSTRAP = "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092";
    private static final String SCHEMA_REGISTRY = "http://43.201.175.172:8081,http://43.202.127.159:8081";
    private static final String TOPIC = "test-topic";
    private static final String GROUP_ID = "kafka-pg-ingestion-" + UUID.randomUUID();

    // ---------- JDBC settings ----------
    private static final String JDBC_URL = "jdbc:postgresql://tgpostgresql.cr4guwc0um8m.ap-northeast-2.rds.amazonaws.com:5432/tgpostgreDB";
    private static final String JDBC_USER = "tgadmin";
    private static final String JDBC_PASS = "p12345678";

    private static final int BATCH_SIZE = 10;

    private static final DateTimeFormatter ISO_FMT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final DateTimeFormatter INPUT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ----- ChatGPT report trigger -----
    private static final int REPORT_INTERVAL = 20;   // generate report every 20 inserted rows
    private static long totalInserted = 0;           // running counter since start

    public static void main(String[] args) throws Exception {
        // 1) Build consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY);
        props.put("specific.avro.reader", true); // return ReceiptData not GenericRecord
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Disable auto-commit; we commit after successful DB insert
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<byte[], ReceiptData> consumer = new KafkaConsumer<>(props);
             Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS)) {

            conn.setAutoCommit(false);

            consumer.subscribe(Collections.singleton(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // nothing
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // jump to the end of each assigned partition
                    consumer.seekToEnd(partitions);
                    // (optional log)
                    partitions.forEach(tp -> {
                        long offset = consumer.position(tp);
                        System.out.printf("🔄 Starting at end offset %d for %s%n", offset, tp);
                    });
                }
            });
            System.out.println("⏳  Listening for records on topic '" + TOPIC + "' …");

            // Prepared-statement for batch insertion
            String sql = "INSERT INTO receipt_raw (" +
                        "franchise_id, store_brand, store_id, store_name, region, store_address, " +
                        "menu_items, total_price, user_id, event_time, user_name, user_gender, user_age" +
                        ") VALUES (?,?,?,?,?,?, ?::jsonb ,?,?,?,?,?,?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int buffer = 0;

                while (true) {
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
        }
    }

    private static void setParams(PreparedStatement ps, ReceiptData r) throws SQLException {
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
        System.out.println("📦 Inserting record: " + r.getStoreBrand() + " | " + r.getStoreName() + " | " + r.getTotalPrice() + "₩");
    }

    private static void flushBatch(PreparedStatement ps, Connection conn, KafkaConsumer<?, ?> consumer) {
        try {
            ps.executeBatch();
            System.out.println("📝 Batch executed. Committing transaction...");
            conn.commit();
            consumer.commitSync();
            System.out.println("✅  committed " + BATCH_SIZE + " rows → PostgreSQL");
            // --- ChatGPT reporting ---
            totalInserted += BATCH_SIZE;
            if (totalInserted % REPORT_INTERVAL == 0) {
                try (GPTReporter reporter = new GPTReporter()) {
                    String report = reporter.buildReport(REPORT_INTERVAL);
                    System.out.println("\n===== 📊 ChatGPT Report =====\n" + report);
                } catch (Exception ge) {
                    System.err.println("⚠️  ChatGPT report failed: " + ge.getMessage());
                }
            }
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignore) {}
            e.printStackTrace();
        }
    }

    private static Instant parseToInstant(String timeStr) {
        try {
            return LocalDateTime.parse(timeStr, INPUT_FMT)
                    .atOffset(ZoneOffset.UTC)
                    .toInstant();
        } catch (DateTimeParseException e) {
            return OffsetDateTime.parse(timeStr, ISO_FMT).toInstant();
        }
    }
}
