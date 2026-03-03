FROM centos:7

# 定義工作目錄
WORKDIR /app

# 1. 建立 ai3 用戶與群組 (確保容器內也有這個用戶識別)
# 如果宿主機的 ai3 UID 不是預設值，建議加上 -u [UID] 以利權限對接
RUN groupadd -r ai3 && useradd -r -g ai3 ai3

# 2. 複製 JAR 檔
COPY hr-import-service-1.0.0.jar app.jar

# 3. 處理權限：將 /app 與 /opt/ai3/ 目錄權限設為 777 讓所有用戶能存取
RUN chown -R ai3:ai3 /app && \
    mkdir -p /opt/ai3 && \
    chmod -R 777 /app && \
    chmod -R 777 /opt/ai3

# 4. 切換為 ai3 用戶執行後續指令
USER ai3

# 使用指定的 JDK 路徑啟動
ENTRYPOINT ["/opt/ai3/java/jdk-17.0.2/bin/java", "-jar", "/app/app.jar"]