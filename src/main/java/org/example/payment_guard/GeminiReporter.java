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
 * Read recent records from {@code receipt_raw} table and ask Gemini‑Pro to create
 * a compact sales report.
 *
 * <p>Requirements:
 * <ul>
 *     <li>Add <b>GEMINI_API_KEY</b> in a local <code>.env</code> file.</li>
 *     <li>Jackson (already on class‑path) for JSON (de)serialisation.</li>
 * </ul>
 *
 * <p>Gemini REST endpoint used here:<br>
 * {@code https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent}</p>
 */
public class GeminiReporter implements AutoCloseable {

    /* ---------- constants ---------- */
    private static final String JDBC_URL =
            "jdbc:postgresql://tgpostgresql.cr4guwc0um8m.ap-northeast-2.rds.amazonaws.com:5432/tgpostgreDB";
    private static final String JDBC_USER = "tgadmin";
    private static final String JDBC_PASS = "p12345678";

    // Use the generally‑available free tier model and the correct host / API version
    private static final String MODEL = "gemini-1.5-flash-latest";
    private static final String GEMINI_ENDPOINT =
            "https://generativelanguage.googleapis.com/v1beta/models/"
            + MODEL + ":generateContent";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    /* ---------- fields ---------- */
    private final String apiKey;
    private final Connection conn;

    /* ---------- constructor ---------- */
    public GeminiReporter() throws SQLException {
        // 1) Load API key from .env
        Dotenv env = Dotenv.configure().ignoreIfMissing().load();
        apiKey = env.get("GEMINI_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("GEMINI_API_KEY not found in .env");
        }

        // 2) DB connection
        conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
    }

    /* ---------- public API ---------- */

    /** Builds a sales report based on the latest {@code limit} rows in <code>receipt_raw</code>. */
    public String buildReport(int limit) throws Exception {
        List<String> lines = fetchRows(limit);

        String prompt = """
            다음은 최근 %d건의 영수증 데이터입니다. 이 데이터를 바탕으로 프랜차이즈별 매출 분석 보고서를 작성해 주세요. 다음 항목들을 포함해 한국어로 정리해 주세요:

            프랜차이즈는 store_brand 보고 구분합니다.
            지점은 store_name을 보고 구분합니다.
            1. 프랜차이즈별 총 매출 순위 (내림차순)
            2. 프랜차이즈별 평균 객단가 비교
            3. 각 프랜차이즈에서 가장 많이 팔린 메뉴 Top-3
            4. 특정 프랜차이즈에서의 이상 거래 또는 특이사항 (예: 너무 큰 주문, 특정 시간대 집중 등)

            데이터:
            %s
            """.formatted(limit, String.join("\n", lines));

        return callGemini(prompt);
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

    /** Do the HTTPS POST to Gemini REST API and return the generated text. */
    private String callGemini(String prompt) throws Exception {
        // Build request body
        ObjectNode root = MAPPER.createObjectNode();
        var contents = root.putArray("contents");
        var contentObj = contents.addObject();
        var parts = contentObj.putArray("parts");
        parts.addObject().put("text", prompt);

        // Basic generation options (temperature low for deterministic output)
        ObjectNode config = root.putObject("generationConfig");
        config.put("temperature", 0.4);

        byte[] payload = MAPPER.writeValueAsBytes(root);

        // Prepare connection
        URL url = new URL(GEMINI_ENDPOINT + "?key=" + apiKey);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
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
                String errMsg = err == null ? "unknown error" :
                        new String(err.readAllBytes(), StandardCharsets.UTF_8);
                throw new RuntimeException("Gemini API error (" + code + "): " + errMsg);
            }
        }

        JsonNode json;
        try (InputStream in = conn.getInputStream()) {
            json = MAPPER.readTree(in);
        }

        // Path: candidates[0].content.parts[0].text
        JsonNode textNode = json
                .path("candidates")
                .path(0)
                .path("content")
                .path("parts")
                .path(0)
                .path("text");

        return textNode.isMissingNode() ? "(no text returned)" : textNode.asText();
    }

    /* ---------- main test ---------- */

    public static void main(String[] args) throws Exception {
        try (GeminiReporter reporter = new GeminiReporter()) {
            String report = reporter.buildReport(10);   // 최신 20건 기준
            System.out.println("\n===== 📊 Gemini Report =====\n" + report);
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