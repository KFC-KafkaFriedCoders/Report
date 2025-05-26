# Han-Noon Report API (통합 서비스)

이 프로젝트는 Kafka 데이터 수집과 RESTful API 리포트 생성을 **하나의 애플리케이션**으로 통합한 서비스입니다.

## 🎯 주요 기능

### 📡 실시간 데이터 수집 (자동 실행)
- `test-topic`에서 메시지 자동 소비
- PostgreSQL의 `receipt_raw` 테이블에 배치 저장 (10건씩)
- 백그라운드에서 지속적으로 실행

### 🌐 REST API 서비스 (포트 8080)
- **리포트 생성**: 20, 50, 100, 250, 500건 선택하여 AI 기반 리포트 생성
- **캐싱**: 100건 이하 요청시 5분간 캐시 활용
- **비동기 처리**: 250건 이상 요청시 비동기 처리
- **완전한 에러 처리**: 데이터 부족, API 오류 등 상세 메시지

## 🚀 실행 방법 (통합)

### 한 번의 명령으로 모든 서비스 실행
```bash
./gradlew bootRun
```

**실행되는 서비스**:
- ✅ Kafka Consumer (백그라운드)
- ✅ REST API Server (포트 8080)
- ✅ 캐시 시스템
- ✅ 비동기 처리

### 실행 확인
```bash
# 헬스 체크
curl http://localhost:8080/api/reports/health

# 리포트 생성 테스트
curl "http://localhost:8080/api/reports/generate?count=20"
```

## 📊 API 엔드포인트

### 리포트 생성
```http
GET /api/reports/generate?count={20|50|100|250|500}
```

**응답 예시 (성공)**:
```json
{
  "success": true,
  "requestedCount": 100,
  "report": "프랜차이즈별 매출 분석 결과...",
  "processingTimeMs": 3250
}
```

**응답 예시 (데이터 부족)**:
```json
{
  "success": false,
  "message": "데이터가 부족합니다! (요청: 500건, 실제: 347건)",
  "requestedCount": 500,
  "actualDataCount": 347
}
```

### 데이터 상태 조회
```http
GET /api/reports/status
```

### 헬스 체크
```http
GET /api/reports/health
```

## ⚡ 성능 최적화

### 캐싱 전략
- **대상**: 100건 이하 요청
- **TTL**: 5분 (쓰기 후), 3분 (액세스 후)
- **용량**: 최대 100개 엔트리

### 비동기 처리
- **대상**: 250건 이상 요청
- **스레드풀**: 코어 2개, 최대 5개
- **타임아웃**: 30초

### 에러 처리
- **타임아웃**: GPT API 호출 30초 제한
- **재시도**: 데이터베이스 연결 실패시 자동 재시도
- **검증**: count 값 및 데이터 충분성 사전 검증

## 🏗️ 아키텍처

```
[purchase-main] → test-topic → [Kafka Consumer] → PostgreSQL
                                                      ↓
[Frontend] → REST API → [GPT Reporter] → AI Report
```

## 📁 프로젝트 구조

```
src/main/java/org/example/payment_guard/
├── ReportApplication.java          # Spring Boot 메인
├── Main.java                      # Kafka Consumer
├── GPTReporter.java              # AI 리포트 생성
├── controller/
│   └── ReportController.java     # REST API 컨트롤러
├── service/
│   └── ReportService.java        # 비즈니스 로직
├── dto/
│   └── ReportResponse.java       # 응답 DTO
├── exception/                    # 커스텀 예외
├── config/                       # 설정 클래스
```

## 🔧 환경 설정

### .env 파일
```
OPENAI_API_KEY=sk-your-api-key-here
```

### application.properties
- 서버 포트: 8080
- 로그 레벨: INFO
- 캐시 설정: Caffeine
- CORS: 모든 오리진 허용

## 🧪 테스트

```bash
# API 서버 헬스 체크
curl http://localhost:8080/api/reports/health

# 데이터 상태 확인
curl http://localhost:8080/api/reports/status

# 리포트 생성 (20건)
curl "http://localhost:8080/api/reports/generate?count=20"
```

## 📊 모니터링

- **Actuator**: `/actuator/health`, `/actuator/info`
- **로그**: 요청/응답, 처리 시간, 에러 상세 정보
- **캐시 통계**: `/actuator/cache`

## 🔗 관련 프로젝트

- `purchase-main`: Excel 데이터를 `test-topic`으로 전송
- `han_noon_front`: 리포트 API를 사용하는 프론트엔드 UI
