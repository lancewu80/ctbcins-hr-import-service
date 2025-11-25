package com.ctbcins.hrimport.service;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

@Service
public class SftpService {
    private static final Logger logger = LoggerFactory.getLogger(SftpService.class);
    
    @Value("${app.sftp.host}")
    private String host;
    
    @Value("${app.sftp.port}")
    private int port;
    
    @Value("${app.sftp.username}")
    private String username;
    
    @Value("${app.sftp.password}")
    private String password;
    
    @Value("${app.sftp.remote-path}")
    private String remotePath;
    
    @Value("${app.sftp.local-dir}")
    private String localDir;
    
    public List<File> downloadFiles() {
        List<File> downloadedFiles = new ArrayList<>();
        Session session = null;
        ChannelSftp channel = null;
        
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(username, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            
            // 確保本地目錄存在
            File localDirectory = new File(localDir);
            if (!localDirectory.exists()) {
                localDirectory.mkdirs();
            }
            
            // 列出遠端檔案
            Vector<ChannelSftp.LsEntry> files = channel.ls(remotePath);
            
            for (ChannelSftp.LsEntry entry : files) {
                if (!entry.getAttrs().isDir() && entry.getFilename().endsWith(".csv")) {
                    String remoteFilePath = remotePath + entry.getFilename();
                    String localFilePath = localDir + File.separator + entry.getFilename();
                    
                    logger.info("下載檔案: {} -> {}", remoteFilePath, localFilePath);
                    
                    try (FileOutputStream fos = new FileOutputStream(localFilePath)) {
                        channel.get(remoteFilePath, fos);
                        downloadedFiles.add(new File(localFilePath));
                        logger.info("成功下載檔案: {}", entry.getFilename());
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("SFTP下載失敗", e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
        
        return downloadedFiles;
    }
}