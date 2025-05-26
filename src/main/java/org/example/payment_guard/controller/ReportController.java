package org.example.payment_guard.controller;

import org.example.payment_guard.dto.ReportResponse;
import org.example.payment_guard.exception.InsufficientDataException;
import org.example.payment_guard.exception.ReportGenerationException;
import org.example.payment_guard.service.ReportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/reports")
@CrossOrigin(origins = "*", methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.PUT, RequestMethod.DELETE, RequestMethod.OPTIONS}, allowedHeaders = "*")
public class ReportController {

    private static final Logger logger = LoggerFactory.getLogger(ReportController.class);
    
    private final ReportService reportService;

    public ReportController(ReportService reportService) {
        this.reportService = reportService;
    }

    /**
     * 리포트 생성 API
     * GET /api/reports/generate?count={20|50|100|250|500}
     */
    @GetMapping("/generate")
    public ResponseEntity<?> generateReport(@RequestParam int count, HttpServletRequest request) {
        logger.info("리포트 생성 요청 받음: {} 건", count);
        
        // IP 주소 추출 및 로그 출력
        String clientIp = getClientIpAddress(request);
        logger.info("요청 IP: {}", clientIp);
        long startTime = System.currentTimeMillis();

        try {
            // 1. count 값 유효성 검증
            reportService.validateCount(count);

            // 2. 데이터 충분성 검증
            int totalRecords = reportService.getTotalRecordCount();
            reportService.validateDataSufficiency(count, totalRecords);

            // 3. 리포트 생성 (동기 처리)
            String report = (String) reportService.generateReport(count);

            // 4. 결과 반환
            long processingTime = System.currentTimeMillis() - startTime;
            
            logger.info("리포트 생성 완료: {} 건, 처리시간: {}ms", count, processingTime);
            
            return ResponseEntity.ok(new ReportResponse(
                true, 
                count, 
                report, 
                processingTime
            ));

        } catch (IllegalArgumentException e) {
            logger.warn("잘못된 count 값 요청: {}", count);
            return ResponseEntity.badRequest().body(new ReportResponse(
                false, 
                e.getMessage()
            ));

        } catch (InsufficientDataException e) {
            logger.warn("데이터 부족: 요청={}, 실제={}", e.getRequestedCount(), e.getActualCount());
            return ResponseEntity.badRequest().body(new ReportResponse(
                false, 
                e.getMessage(), 
                e.getRequestedCount(), 
                e.getActualCount()
            ));

        } catch (ReportGenerationException e) {
            logger.error("리포트 생성 중 오류 발생", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ReportResponse(
                false, 
                "리포트 생성 중 오류가 발생했습니다: " + e.getMessage()
            ));

        } catch (Exception e) {
            logger.error("예상치 못한 오류 발생", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ReportResponse(
                false, 
                "서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요."
            ));
        }
    }

    /**
     * 현재 데이터 상태 조회 API
     * GET /api/reports/status
     */
    @GetMapping("/status")
    public ResponseEntity<Object> getReportStatus() {
        try {
            int recordCount = reportService.getTotalRecordCount();
            
            return ResponseEntity.ok(new Object() {
                public final boolean success = true;
                public final int totalRecords = recordCount;
                public final String message = String.format("현재 총 %d건의 데이터가 있습니다.", recordCount);
            });
            
        } catch (Exception e) {
            logger.error("상태 조회 중 오류 발생", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ReportResponse(
                false, 
                "상태 조회 중 오류가 발생했습니다."
            ));
        }
    }

    /**
     * 헬스 체크 API
     * GET /api/reports/health
     */
    @GetMapping("/health")
    public ResponseEntity<Object> healthCheck() {
        return ResponseEntity.ok(new Object() {
            public final String status = "UP";
            public final long timestamp = System.currentTimeMillis();
            public final String message = "Report API is running";
        });
    }
    
    /**
     * 클라이언트 IP 주소를 추출하는 메서드
     * 프록시나 로드밸런서를 고려하여 실제 클라이언트 IP를 가져옴
     */
    private String getClientIpAddress(HttpServletRequest request) {
        String xForwardedForHeader = request.getHeader("X-Forwarded-For");
        if (xForwardedForHeader == null || xForwardedForHeader.isEmpty()) {
            return request.getRemoteAddr();
        } else {
            // X-Forwarded-For 헤더에서 첫 번째 IP 추출 (실제 클라이언트 IP)
            return xForwardedForHeader.split(",")[0].trim();
        }
    }
}
