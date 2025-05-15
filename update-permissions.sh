#!/bin/bash

# 모든 스크립트 파일에 실행 권한 부여
chmod +x run.sh
chmod +x run-direct.sh 
chmod +x run-java.sh
chmod +x create-topics.sh
chmod +x chmod-scripts.sh
chmod +x update-permissions.sh

# 확인 메시지 출력
echo "모든 스크립트 파일에 실행 권한이 부여되었습니다."
echo "다음 중 하나의 방법으로 실행하세요:"
echo "1. ./run.sh - 기본 Gradle 실행 방식"
echo "2. ./run-direct.sh - JAR 생성 후 직접 실행"
echo "3. ./run-java.sh - 컴파일된 클래스 직접 실행"
