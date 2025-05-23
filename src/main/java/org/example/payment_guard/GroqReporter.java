package org.example.payment_guard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class GroqReporter implements AutoCloseable {

    private static final String JDBC_URL = "jdbc:postgresql://tgpostgresql.cr4guwc0um8m.ap-northeast-2.rds.amazonaws.com:5432/tgpostgreDB";
    private static final String JDBC_USER = "tgadmin";
    private static final String JDBC_PASS = "p12345678";

    private static final String GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    // Available Groq models: https://console.groq.com/docs/models
    private static final String GROQ_MODEL = "llama3-70b-8192";

    private final String apiKey;
    private final Connection conn;

    public GroqReporter() throws SQLException {
        Dotenv env = Dotenv.configure().ignoreIfMissing().load();
        apiKey = env.get("GROQ_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("GROQ_API_KEY not found in .env");
        }
        conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
    }

    public String buildReport(int limit) throws Exception {
        List<String> lines = fetchRows(limit);
        String prompt = """
            다음은 최근 %d건의 영수증 데이터입니다. 이 데이터를 바탕으로 **프랜차이즈별 매출 분석 보고서**를 작성해 주세요. 다음 항목들을 포함해 **한국어로 정리해 주세요**:

            프랜차이즈는 store_brand 보고 구분합니다.
            지점은 store_name을 보고 구분합니다.
            1. 프랜차이즈별 총 매출 순위 (내림차순)
            2. 프랜차이즈별 평균 객단가 비교
            3. 각 프랜차이즈에서 가장 많이 팔린 메뉴 Top-3
            4. 특정 프랜차이즈에서의 이상 거래 또는 특이사항 (예: 너무 큰 주문, 특정 시간대 집중 등)

            데이터:
            %s
            """.formatted(limit, String.join("\n", lines));
        return callGroq(prompt);
    }

    private List<String> fetchRows(int limit) throws SQLException {
        String sql = """
                SELECT store_brand, store_name, total_price, event_time, menu_items
                FROM receipt_raw
                ORDER BY event_time DESC
                LIMIT ?
                """;
        List<String> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(String.format("- %s | %s | %,d원 | %s | 메뉴=%s",
                            rs.getString("store_brand"),
                            rs.getString("store_name"),
                            rs.getInt("total_price"),
                            rs.getTimestamp("event_time").toLocalDateTime().format(TS_FMT),
                            rs.getString("menu_items")));
                }
            }
        }
        return result;
    }

    private String callGroq(String prompt) throws Exception {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("model", GROQ_MODEL);           // use chosen Groq model
        body.put("temperature", 0.4);            // deterministic output
        var messages = body.putArray("messages");
        var msg = messages.addObject();
        msg.put("role", "user");
        msg.put("content", prompt);

        byte[] payload = MAPPER.writeValueAsBytes(body);

        URL url = new URL(GROQ_API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + apiKey);
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload);
        }

        int code = conn.getResponseCode();
        if (code != 200) {
            try (InputStream err = conn.getErrorStream()) {
                String errMsg = err == null ? "unknown error" :
                        new String(err.readAllBytes(), StandardCharsets.UTF_8);
                throw new RuntimeException("Groq API error (" + code + "): " + errMsg);
            }
        }

        JsonNode json;
        try (InputStream in = conn.getInputStream()) {
            json = MAPPER.readTree(in);
        }

        return json.path("choices").path(0).path("message").path("content").asText("(no text returned)");
    }

    public static void main(String[] args) throws Exception {
        try (GroqReporter reporter = new GroqReporter()) {
            String report = reporter.buildReport(10);
            System.out.println("\n===== 📊 Groq Report =====\n" + report);
        }
    }

    @Override
    public void close() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException ignore) {
        }
    }
}