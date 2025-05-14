#!/bin/bash

# 필요한 토픽을 생성하는 스크립트

# 토픽 존재 여부 확인
if docker exec broker kafka-topics --bootstrap-server broker:9092 --list | grep -q "3_non_response"; then
    echo "✅ '3_non_response' 토픽이 이미 존재합니다."
else
    echo "🔧 '3_non_response' 토픽을 생성합니다..."
    docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic 3_non_response --partitions 1 --replication-factor 1
    echo "✅ '3_non_response' 토픽이 생성되었습니다."
fi

echo "모든 준비가 완료되었습니다. 이제 다음 명령어를 실행하여 Payment Guard 애플리케이션을 시작하세요:"
echo "./gradlew run"
