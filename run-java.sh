#!/bin/bash

# JVM 모듈 접근 제한 우회 방법
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
           -Djdk.reflect.useDirectMethodHandle=false"

# 클래스패스와 메인 클래스 설정
MAIN_CLASS="org.example.payment_guard.Main"
CLASSPATH="./build/classes/java/main:./build/resources/main"

# 의존성 추가
for jar in $(find ~/.gradle -name "*.jar"); do
  CLASSPATH="$CLASSPATH:$jar"
done

echo "실행 중: java $JAVA_OPTS -cp $CLASSPATH $MAIN_CLASS"
java $JAVA_OPTS -cp $CLASSPATH $MAIN_CLASS
