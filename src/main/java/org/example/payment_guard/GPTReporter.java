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

/**
 * Read recent records from {@code receipt_raw} table and ask ChatGPT (OpenAI) to create
 * a compact sales report.
 *
 * <p>Requirements:
 * <ul>
 *     <li>Add <b>OPENAI_API_KEY</b> in a local <code>.env</code> file.</li>
 *     <li>Jackson (already on class-path) for JSON (de)serialisation.</li>
 * </ul>
 *
 * <p>OpenAI REST endpoint used here:<br>
 * {@code https://api.openai.com/v1/chat/completions}</p>
 */
public class GPTReporter implements AutoCloseable {

    private static final String MODEL = "gpt-4o-mini";
    private static final String OPENAI_ENDPOINT = "https://api.openai.com/v1/chat/completions";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private final String apiKey;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPass;
    private final Connection conn;

    public GPTReporter() throws SQLException {
        Dotenv env = Dotenv.configure().ignoreIfMissing().load();
        
        apiKey = env.get("OPENAI_API_KEY");
        jdbcUrl = env.get("DB_URL");
        jdbcUser = env.get("DB_USER");
        jdbcPass = env.get("DB_PASSWORD");
        
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("OPENAI_API_KEY not found in .env");
        }
        if (jdbcUrl == null || jdbcUser == null || jdbcPass == null) {
            throw new IllegalStateException("DB 환경변수가 설정되지 않았습니다. .env 파일을 확인해주세요.");
        }
        
        conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
    }

    /* ---------- public API ---------- */

    /** Builds a sales report based on the latest {@code limit} rows in <code>receipt_raw</code>. */
    public String buildReport(int limit) throws Exception {
        List<String> lines = fetchRows(limit);

        String prompt = """
            다음은 최근 %d건의 영수증 데이터입니다. 이 데이터를 바탕으로 프랜차이즈별 매출 분석 보고서를 작성해 주세요. 다음 항목들을 포함해 한국어로 정리해 주세요:

            프랜차이즈는 store_brand로 구분합니다.
            지점은 store_name으로 구분합니다.

            1. 프랜차이즈별 총 매출 순위 (내림차순)
            2. 프랜차이즈별 평균 객단가 비교
            3. 각 프랜차이즈에서 가장 많이 팔린 메뉴 Top-3
            4. 이상 거래 또는 특이사항 (예: 너무 큰 주문, 특정 시간대 집중 등)

            데이터:
            %s
            """.formatted(limit, String.join("\n", lines));

        return callChatGPT(prompt);
    }

    /* ---------- helper methods ---------- */

    /** Query Postgres and format each row as a single line of text. */
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

    /** Do the HTTPS POST to OpenAI Chat API and return the generated text. */
    private String callChatGPT(String prompt) throws Exception {
        // Build request body
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", MODEL);
        var messages = root.putArray("messages");
        ObjectNode sys = messages.addObject();   // system role (optional)
        sys.put("role", "system");
        sys.put("content", "You are a helpful assistant specialized in concise sales analytics.");

        ObjectNode user = messages.addObject();
        user.put("role", "user");
        user.put("content", prompt);

        // 조금 더 일관된 출력이 필요하면 temperature를 낮춘다
        root.put("temperature", 0.4);

        byte[] payload = MAPPER.writeValueAsBytes(root);

        // Prepare connection
        URL url = new URL(OPENAI_ENDPOINT);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        conn.setRequestProperty("Authorization", "Bearer " + apiKey);
        conn.setDoOutput(true);
        conn.setConnectTimeout(15_000);
        conn.setReadTimeout(30_000);

        // Send body
        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload);
        }

        // Read response
        int code = conn.getResponseCode();
        if (code != 200) {
            try (InputStream err = conn.getErrorStream()) {
                String errMsg = err == null ? "unknown error"
                        : new String(err.readAllBytes(), StandardCharsets.UTF_8);
                throw new RuntimeException("OpenAI API error (" + code + "): " + errMsg);
            }
        }

        JsonNode json;
        try (InputStream in = conn.getInputStream()) {
            json = MAPPER.readTree(in);
        }

        // Path: choices[0].message.content
        JsonNode textNode = json
                .path("choices")
                .path(0)
                .path("message")
                .path("content");

        return textNode.isMissingNode() ? "(no text returned)" : textNode.asText().trim();
    }

    /* ---------- quick test ---------- */
    public static void main(String[] args) throws Exception {
        try (GPTReporter reporter = new GPTReporter()) {
            System.out.println(reporter.buildReport(10));
        }
    }

    /** Close the underlying PostgreSQL connection */
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
