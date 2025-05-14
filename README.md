# Payment Guard

이 프로젝트는 Kafka 토픽 간 데이터 파이프라인을 구성하는 애플리케이션입니다.

## 기능

- `test-topic`에서 메시지를 소비
- 수신한 메시지를 변환 없이 `3_non_response` 토픽으로 전송
- 이로써 테스트 데이터를 실제 비즈니스 파이프라인에 흘려보낼 수 있음

## 사전 요구사항

- Java 17 이상
- Gradle
- Docker 및 Docker Compose
- purchase-main 프로젝트가 실행 중이고 `test-topic`에 데이터를 전송 중이어야 함

## 실행 방법

1. 권한 부여
   ```bash
   sh chmod-scripts.sh
   ```

2. 토픽 생성 및 애플리케이션 실행
   ```bash
   ./run.sh
   ```

## 토픽 확인 방법

Kafka Control Center UI에서 토픽 데이터를 확인할 수 있습니다.
- 접속 URL: http://localhost:9021
- Topics 메뉴에서 `3_non_response` 토픽 선택
- Messages 탭에서 메시지 확인

## 아키텍처

```
[purchase-main]      [payment_guard-main]
Excel 데이터 ---> test-topic ---> 3_non_response
```

## 관련 프로젝트

- `purchase-main`: Excel 데이터를 `test-topic`으로 보내는 애플리케이션
- `payment_guard-main`: `test-topic`에서 데이터를 소비하여 `3_non_response`로 보내는 애플리케이션
