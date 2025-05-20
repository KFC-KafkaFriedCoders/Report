#!/bin/bash

echo "Starting Raw Data Processor..."

# 현재 디렉토리 확인
CURRENT_DIR=$(pwd)
echo "Current directory: $CURRENT_DIR"

# Gradle을 사용하여 RawDataProcessor 직접 실행
./gradlew run -PmainClass=org.example.payment_guard.RawDataProcessor

echo "Raw Data Processor finished."