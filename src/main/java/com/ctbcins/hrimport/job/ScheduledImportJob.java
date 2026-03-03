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

    /** 本機要處理的 CSV 檔名（只處理此檔案） */
    @Value("${scheduler.hrimport.filename:HrImport.csv}")
    private String hrInputFilename;

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

        logger.info("開始執行 HR 匯入（本機路徑） localPath={} filename={}", localImportPath, hrInputFilename);
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
                    //file.delete();
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
     * 從本機路徑直接匯入 CSV（只處理設定的檔名）
     */
    private void runLocalImportJob() {
        try {
            File folder = new File(localImportPath);

            if (!folder.exists() || !folder.isDirectory()) {
                logger.warn("Local import path does not exist or is not a directory: {}", localImportPath);
                return;
            }

            File targetFile = new File(folder, hrInputFilename);

            if (!targetFile.exists() || !targetFile.isFile()) {
                logger.info("指定的 CSV 檔案不存在於 local path: {} (filename={})", localImportPath, hrInputFilename);
                return;
            }

            try {
                logger.info("處理本機 CSV 檔案: {}", targetFile.getName());
                hrDataProcessService.processHRFile(targetFile.getAbsolutePath());
                //targetFile.delete();
            } catch (Exception e) {
                logger.error("處理本機檔案失敗: {}", targetFile.getName(), e);
            }

            logger.info("本機 HR 匯入完成，處理檔案: {}", targetFile.getName());

        } catch (Exception e) {
            logger.error("本機 HR 匯入任務失敗", e);
        }
    }
}
