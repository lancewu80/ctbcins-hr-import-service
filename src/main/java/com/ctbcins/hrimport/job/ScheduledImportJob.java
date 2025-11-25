package com.ctbcins.hrimport.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ctbcins.hrimport.service.HRDataProcessService;
import com.ctbcins.hrimport.service.SftpService;

import java.io.File;

@Component
public class ScheduledImportJob {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledImportJob.class);

    @Autowired
    private SftpService sftpService;

    @Autowired
    private HRDataProcessService hrDataProcessService;

    /** 是否啟用 minute job */
    @Value("${scheduler.hrimport.enabled:true}")
    private boolean schedulerEnabled;

    /** minute job 的 cron 表達式 */
    @Value("${scheduler.hrimport.cron:0 * * * * *}")
    private String cronExpression;

    /** 本機 CSV 路徑（由 application.yml 設定） */
    @Value("${scheduler.hrimport.localPath:./hr-import}")
    private String localImportPath;

    /**
     * 每日固定 2:00 — SFTP 匯入
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public void dailyHRImport() {
        logger.info("開始執行每日 HR 匯入（SFTP）");
        runSftpImportJob();
    }

    /**
     * minute job — 改為從本機指定路徑匯入 CSV
     */
    @Scheduled(cron = "${scheduler.hrimport.cron:0 * * * * *}")
    public void minuteImportJob() {
        if (!schedulerEnabled) {
            logger.info("HR Import minute job is disabled.");
            return;
        }

        logger.info("開始執行 HR 匯入（本機路徑） localPath={}", localImportPath);
        runLocalImportJob();
    }

    /**
     * SFTP 匯入（原本邏輯）
     */
    private void runSftpImportJob() {
        try {
            var downloadedFiles = sftpService.downloadFiles();

            for (File file : downloadedFiles) {
                try {
                    hrDataProcessService.processHRFile(file.getAbsolutePath());
                    file.delete();
                } catch (Exception e) {
                    logger.error("處理檔案失敗: {}", file.getName(), e);
                }
            }

            logger.info("每日 HR 匯入完成，處理檔案數: {}", downloadedFiles.size());
        } catch (Exception e) {
            logger.error("每日 HR 匯入任務失敗", e);
        }
    }

    /**
     * 從本機路徑直接匯入 CSV
     */
    private void runLocalImportJob() {
        try {
            File folder = new File(localImportPath);

            if (!folder.exists() || !folder.isDirectory()) {
                logger.warn("Local import path does not exist or is not a directory: {}", localImportPath);
                return;
            }

            File[] csvFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

            if (csvFiles == null || csvFiles.length == 0) {
                logger.info("No CSV files found in local path: {}", localImportPath);
                return;
            }

            for (File file : csvFiles) {
                try {
                    logger.info("處理本機 CSV 檔案: {}", file.getName());
                    hrDataProcessService.processHRFile(file.getAbsolutePath());
                    file.delete();
                } catch (Exception e) {
                    logger.error("處理本機檔案失敗: {}", file.getName(), e);
                }
            }

            logger.info("本機 HR 匯入完成，共處理 {} 個 CSV", csvFiles.length);

        } catch (Exception e) {
            logger.error("本機 HR 匯入任務失敗", e);
        }
    }
}
