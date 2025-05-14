#!/bin/bash

# Payment Guard 애플리케이션 실행 스크립트

# 토픽 생성 확인
./create-topics.sh

# 애플리케이션 실행
echo "Payment Guard 애플리케이션을 시작합니다..."
./gradlew run
