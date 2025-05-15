#!/bin/bash

# 디버그 모드로 실행하기 위한 스크립트

# 토픽 생성 확인
./create-topics.sh

echo "Flink 애플리케이션을 디버그 모드로 실행합니다..."
echo "Gradle 로그 레벨을 INFO로 설정하여 더 많은 정보를 확인할 수 있습니다."

# 디버그 옵션으로 실행
./gradlew run --info
