中信產多元理賠收件平台 - 人資資料匯入服務

專案概述

本專案為中信產2024年多元理賠收件平台之人資資料匯入服務，負責每日定時從SFTP伺服器下載人資CSV檔案，並將部門及員工資料匯入至多元理賠收件平台資料庫。



主要功能

✅ SFTP檔案下載：自動從SFTP伺服器下載人資CSV檔案



✅ 部門資料處理：自動拆解多層部門結構並建立部門關係



✅ 員工資料處理：根據部門層級智慧更新員工資料



✅ 定時任務：每日自動執行匯入作業



✅ 錯誤處理：完善的異常處理和日誌記錄



✅ 容器化部署：支援Docker容器化部署



技術架構

後端框架: Spring Boot 2.7.18



資料庫: Microsoft SQL Server



任務排程: Spring Scheduler



檔案傳輸: SFTP (JSch)



CSV處理: OpenCSV



容器化: Docker + Docker Compose



建置工具: Maven



系統需求

Java 11+



Maven 3.6+



Docker 20.10+ (可選)



SQL Server 2019+



快速開始

1\. 環境設定

複製專案並設定環境變數：



bash

git clone <repository-url>

cd hr-import-service

2\. 資料庫設定

確保SQL Server資料庫中存在以下表格：



TsUser - 員工資料表



TsAccount - 帳號資料表



TsAccountIdentity - 帳號身份關聯表



CUS\_HRImport\_Department - 部門資料表（專案啟動時自動建立）



3\. 組態設定

修改 src/main/resources/application.yml：



yaml

spring:

&nbsp; datasource:

&nbsp;   url: jdbc:sqlserver://localhost:1433;databaseName=ecp8502

&nbsp;   username: your-username

&nbsp;   password: your-password



app:

&nbsp; sftp:

&nbsp;   host: 10.2.3.4

&nbsp;   port: 22

&nbsp;   username: ftpuser

&nbsp;   password: ai3\[1qaz@WSX]

&nbsp;   remote-path: /hrfile/

4\. 建置與執行

使用Maven執行：

bash

mvn clean package

java -jar target/hr-import-service-1.0.0.jar

使用Docker執行：

bash

docker-compose up -d

資料處理邏輯

部門處理

將DEP\_NAME以「-」拆解成多層部門結構



自動計算部門層級(tree\_level)



建立部門間的父子關係



員工處理

以WORKCARD為員工唯一識別碼



部門層級 < 4時更新員工部門



部門層級 ≥ 4時保留原部門不變



狀態為A(在職)的員工才會被處理



檔案格式

CSV檔案需包含以下欄位：



csv

CPNYID,DEP\_NO,DEP\_CODE,DEP\_NAME,STATE\_NO,STATE\_NAME,EMP\_ID,EMP\_NAME,WORKCARD,MOBILE,...

定時任務設定

每日匯入: 凌晨2點執行 (0 0 2 \* \* ?)



測試任務: 每5分鐘執行 (0 \*/5 \* \* \* ?)



API端點

手動觸發匯入

http

POST /api/import/trigger

Content-Type: application/json



{

&nbsp; "filePath": "/path/to/local/file.csv"

}

檢查服務狀態

http

GET /api/health

部署說明

Docker部署

建置Docker映像：



bash

docker build -t hr-import-service:latest .

使用Docker Compose啟動：



bash

docker-compose up -d

環境變數

變數名稱	說明	預設值

SPRING\_DATASOURCE\_URL	資料庫連線URL	-

SPRING\_DATASOURCE\_USERNAME	資料庫使用者名稱	-

SPRING\_DATASOURCE\_PASSWORD	資料庫密碼	-

SFTP\_HOST	SFTP主機位址	10.2.3.4

SFTP\_PORT	SFTP連接埠	22

SFTP\_USERNAME	SFTP使用者名稱	ftpuser

SFTP\_PASSWORD	SFTP密碼	-

SFTP\_PATH	SFTP檔案路徑	/hrfile/

監控與日誌

日誌檔案：logs/hr-import.log



應用程式日誌級別：DEBUG



資料庫SQL日誌：啟用



故障排除

常見問題

SFTP連線失敗



檢查網路連線和防火牆設定



確認SFTP憑證正確



資料庫連線失敗



確認資料庫服務運行中



檢查連線字串和憑證



CSV檔案解析錯誤



確認檔案編碼為UTF-8



檢查CSV格式是否符合規範



日誌查詢

bash

\# 查看應用程式日誌

tail -f logs/hr-import.log



\# Docker容器日誌

docker logs hr-import-service

專案結構

text

hr-import-service/

├── src/main/java/com/citic/hrimport/

│   ├── HrImportApplication.java      # 應用程式入口

│   ├── config/                       # 組態設定

│   ├── entity/                       # 資料實體

│   ├── repository/                   # 資料存取層

│   ├── service/                      # 業務邏輯層

│   ├── job/                          # 定時任務

│   └── dto/                          # 資料傳輸物件

├── docker/

│   ├── Dockerfile                    # Docker建置檔案

│   └── docker-compose.yml           # Docker Compose設定

├── workflows/

│   └── deploy.yml                   # GitHub Actions工作流

└── pom.xml                          # Maven依賴管理

開發指引

新增功能

在對應的package中建立新的類別



遵循Spring Boot的依賴注入原則



添加適當的單元測試



更新相關的組態設定



測試

bash

\# 執行單元測試

mvn test



\# 執行整合測試

mvn verify

版本資訊

v1.0.0 (2024-11-25)



初始版本發布



實現基本的人資資料匯入功能



支援SFTP檔案下載和CSV處理



容器化部署支援



聯絡資訊

如有問題請聯繫系統管理員或開發團隊。





