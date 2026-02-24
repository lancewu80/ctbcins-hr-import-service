FROM centos:7

WORKDIR /app

# 複製 JAR
COPY hr-import-service-1.0.0.jar app.jar

# 使用主機已存在的 JDK
ENTRYPOINT ["/opt/ai3/java/jdk-17.0.2/bin/java", "-jar", "/app/app.jar"]
