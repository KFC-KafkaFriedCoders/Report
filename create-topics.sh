#!/bin/bash

# 필요한 토픽을 생성하는 스크립트

# 토픽 존재 여부 확인
if docker exec broker kafka-topics --bootstrap-server broker:9092 --list | grep -q "3_non_response"; then
    echo "\u2705 '3_non_response' 토픽이 이미 존재합니다."
else
    echo "\ud83d\udd27 '3_non_response' 토픽을 생성합니다..."
    docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic 3_non_response --partitions 1 --replication-factor 1
    echo "\u2705 '3_non_response' 토픽이 생성되었습니다."
fi

# test-topic 확인
if docker exec broker kafka-topics --bootstrap-server broker:9092 --list | grep -q "test-topic"; then
    echo "\u2705 'test-topic' 토픽이 존재합니다."
else
    echo "\u26a0\ufe0f 'test-topic'이 없습니다. purchase-main 애플리케이션이 실행 중인지 확인하세요."
fi

echo "\ud83d\udd14 모든 준비가 완료되었습니다. Store Inactivity Detector 애플리케이션을 시작합니다:"
echo "./run.sh"
