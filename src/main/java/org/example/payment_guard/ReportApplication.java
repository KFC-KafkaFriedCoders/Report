package org.example.payment_guard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableCaching
@EnableAsync
public class ReportApplication {

    private static final Logger logger = LoggerFactory.getLogger(ReportApplication.class);

    public static void main(String[] args) {
        logger.info("🚀 Han-Noon Report API 시작...");
        logger.info("📡 Kafka Consumer + REST API 통합 실행");
        
        SpringApplication.run(ReportApplication.class, args);
        
        logger.info("✅ 애플리케이션 시작 완료!");
        logger.info("🌐 REST API: http://localhost:8080/api/reports/health");
        logger.info("📊 리포트 생성: http://localhost:8080/api/reports/generate?count=20");
    }
}
