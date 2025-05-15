#!/bin/bash

# Store Inactivity Detector 응급 실행 스크립트 (메모리 최적화)

# 토픽 생성 확인
./create-topics.sh

echo "Store Inactivity Detector 응급 실행 스크립트 (Out of Memory 해결)"
echo "메모리 최적화 설정으로 애플리케이션을 실행합니다..."
echo "----------------------------------------"

# 메모리 옵션을 명시적으로 지정하여 Java로 직접 실행
java -Xmx512m -Xms256m \
     -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:ParallelGCThreads=4 \
     -XX:ConcGCThreads=2 -XX:InitiatingHeapOccupancyPercent=70 \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/java.text=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
     -cp "build/classes/java/main:build/resources/main:$(find ~/.gradle -name '*.jar' | tr '\n' ':')" \
     org.example.payment_guard.Main
