#!/bin/bash

# Store Inactivity Detector 애플리케이션 다이렉트 실행 스크립트

# 토픽 생성 확인
./create-topics.sh

# 애플리케이션 실행
echo "Store Inactivity Detector 애플리케이션을 시작합니다..."
echo "30초 동안 활동이 없는 상점(store)을 감지합니다."
echo "----------------------------------------"
echo "- 플리크 에 의해 상태가 관리되며 비활성 상태가 감지되면 알림을 생성합니다."
echo "- 생성된 알림은 3_non_response 토픽으로 전송됩니다."
echo "- http://localhost:9021 에서 토픽 데이터를 확인할 수 있습니다."
echo "----------------------------------------"

# 클래스패스 설정
./gradlew jar

CLASSPATH="./build/libs/payment_guard-1.0-SNAPSHOT.jar:$(find ~/.gradle -name '*.jar' | tr '\n' ':')"

# 메인 클래스 설정
MAIN_CLASS="org.example.payment_guard.Main"

# Java 실행 옵션
JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED \
           --add-opens=java.base/java.lang=ALL-UNNAMED \
           --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
           --add-opens=java.base/java.text=ALL-UNNAMED \
           --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
           --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
           --add-opens=java.base/java.nio=ALL-UNNAMED \
           --add-opens=java.base/java.util.stream=ALL-UNNAMED \
           --add-opens=java.base/java.util.function=ALL-UNNAMED \
           --add-opens=java.base/java.time=ALL-UNNAMED \
           --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
           -Djdk.reflect.useDirectMethodHandle=false \
           -Dorg.apache.flink.serializer.useDirectBufferThreshold=0 \
           -Xmx2048m -Xms512m"

# 직접 자바로 실행
java $JAVA_OPTS -cp $CLASSPATH $MAIN_CLASS
