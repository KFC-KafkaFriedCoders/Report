package org.example.payment_guard.service;

import io.github.cdimascio.dotenv.Dotenv;
import org.example.payment_guard.GPTReporter;
import org.example.payment_guard.exception.InsufficientDataException;
import org.example.payment_guard.exception.ReportGenerationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class ReportService {

    private static final Logger logger = LoggerFactory.getLogger(ReportService.class);
    
    private static final List<Integer> ALLOWED_COUNTS = Arrays.asList(20, 50, 100, 250, 500);
    private static final long GPT_TIMEOUT_SECONDS = 60;
    
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPass;

    public ReportService() {
        Dotenv env = Dotenv.configure().ignoreIfMissing().load();
        this.jdbcUrl = env.get("DB_URL");
        this.jdbcUser = env.get("DB_USER");
        this.jdbcPass = env.get("DB_PASSWORD");
        
        if (jdbcUrl == null || jdbcUser == null || jdbcPass == null) {
            throw new IllegalStateException("DB 환경변수가 설정되지 않았습니다. .env 파일을 확인해주세요.");
        }
    }

    /**
     * count 값 유효성 검증
     */
    public void validateCount(int count) {
        if (!ALLOWED_COUNTS.contains(count)) {
            throw new IllegalArgumentException(
                String.format("허용되지 않은 count 값입니다. (%s 중 선택)", ALLOWED_COUNTS.toString())
            );
        }
    }

    /**
     * 데이터베이스의 총 레코드 수 조회 (브랜드별)
     */
    public int getTotalRecordCount(String brand) {
        String sql;
        if ("전체".equals(brand)) {
            sql = "SELECT COUNT(*) FROM receipt_raw";
        } else {
            sql = "SELECT COUNT(*) FROM receipt_raw WHERE store_brand = ?";
        }
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            if (!"전체".equals(brand)) {
                ps.setString(1, brand);
            }
            
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            }
            
        } catch (SQLException e) {
            logger.error("데이터베이스 연결 또는 조회 중 오류 발생", e);
            throw new ReportGenerationException("데이터베이스 조회 중 오류가 발생했습니다.", e);
        }
    }

    /**
     * 데이터베이스의 총 레코드 수 조회 (전체 - 호환성)
     */
    public int getTotalRecordCount() {
        return getTotalRecordCount("전체");
    }

    /**
     * 데이터 충분성 검증
     */
    public void validateDataSufficiency(int requestedCount, int actualCount) {
        if (actualCount < requestedCount) {
            throw new InsufficientDataException(requestedCount, actualCount);
        }
    }

    /**
     * 캐시를 사용한 동기 리포트 생성 (브랜드별)
     */
    @Cacheable(value = "reports", key = "#count + '_' + #brand", condition = "#count <= 500")
    public String generateReportSync(int count, String brand) {
        logger.info("동기 리포트 생성 시작: {} 건, 브랜드: {}", count, brand);
        
        try (GPTReporter reporter = new GPTReporter()) {
            String report = reporter.buildReport(count, brand);
            logger.info("동기 리포트 생성 완료: {} 건, 브랜드: {}", count, brand);
            return report;
            
        } catch (Exception e) {
            logger.error("GPT 리포트 생성 중 오류 발생", e);
            throw new ReportGenerationException("리포트 생성 중 오류가 발생했습니다: " + e.getMessage(), e);
        }
    }

    /**
     * 캐시를 사용한 동기 리포트 생성 (호환성)
     */
    @Cacheable(value = "reports", key = "#count + '_전체'", condition = "#count <= 500")
    public String generateReportSync(int count) {
        return generateReportSync(count, "전체");
    }

    /**
     * 비동기 리포트 생성 (큰 데이터셋용) - 브랜드별
     */
    @Async
    public CompletableFuture<String> generateReportAsync(int count, String brand) {
        logger.info("비동기 리포트 생성 시작: {} 건, 브랜드: {}", count, brand);
        
        try (GPTReporter reporter = new GPTReporter()) {
            // 타임아웃 처리를 위한 CompletableFuture
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return reporter.buildReport(count, brand);
                } catch (Exception e) {
                    throw new RuntimeException("리포트 생성 실패", e);
                }
            });
            
            // 타임아웃 설정
            String report = future.get(GPT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            logger.info("비동기 리포트 생성 완료: {} 건, 브랜드: {}", count, brand);
            return CompletableFuture.completedFuture(report);
            
        } catch (Exception e) {
            logger.error("비동기 GPT 리포트 생성 중 오류 발생", e);
            throw new ReportGenerationException("리포트 생성 중 오류가 발생했습니다: " + e.getMessage(), e);
        }
    }

    /**
     * 비동기 리포트 생성 (호환성)
     */
    @Async
    public CompletableFuture<String> generateReportAsync(int count) {
        return generateReportAsync(count, "전체");
    }

    /**
     * count에 따라 동기/비동기 선택하여 리포트 생성 - 브랜드별
     */
    public Object generateReport(int count, String brand) {
        // 모든 요청을 동기 처리 (비동기 기능 비활성화)
        return generateReportSync(count, brand);
    }

    /**
     * count에 따라 동기/비동기 선택하여 리포트 생성 (호환성)
     */
    public Object generateReport(int count) {
        return generateReport(count, "전체");
    }
}
