package com.ctbcins.hrimport.service;

import com.ctbcins.hrimport.dto.HRData;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class HRDataProcessService {
    private static final Logger logger = LoggerFactory.getLogger(HRDataProcessService.class);
    
    @Autowired
    private EntityManager entityManager;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Value("${app.processing.enabled-states:A}")
    private String enabledStates;
    
    @Transactional
    public void processHRFile(String filePath) {
        logger.info("開始處理HR檔案: {}", filePath);
        
        try (Reader reader = new FileReader(filePath, StandardCharsets.UTF_8)) {
            // 設定CSV映射策略
            HeaderColumnNameMappingStrategy<HRData> strategy = 
                new HeaderColumnNameMappingStrategy<>();
            strategy.setType(HRData.class);
            
            CsvToBean<HRData> csvToBean = new CsvToBeanBuilder<HRData>(reader)
                    .withMappingStrategy(strategy)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build();
            
            List<HRData> hrDataList = csvToBean.parse();
            
            // 過濾有效資料（移除空行和無效資料）
            List<HRData> validData = hrDataList.stream()
                    .filter(data -> data.getDepCode() != null && !data.getDepCode().trim().isEmpty())
                    .filter(data -> data.getEmpName() != null && !data.getEmpName().trim().isEmpty())
                    .filter(data -> enabledStates.contains(data.getStateNo()))
                    .collect(Collectors.toList());
            
            logger.info("CSV檔案解析完成，總記錄數: {}, 有效記錄數: {}", 
                hrDataList.size(), validData.size());
            
            if (validData.isEmpty()) {
                logger.warn("沒有有效的HR資料需要處理");
                return;
            }
            
            // 處理部門資料
            processDepartments(validData);
            
            // 處理員工資料
            processEmployees(validData);
            
            logger.info("成功處理HR資料檔案: {}, 有效記錄數: {}", filePath, validData.size());
            
        } catch (Exception e) {
            logger.error("處理HR檔案失敗: {}", filePath, e);
            throw new RuntimeException("HR檔案處理失敗", e);
        }
    }
    
    private void processDepartments(List<HRData> hrDataList) {
        logger.info("開始處理部門資料，共 {} 筆記錄", hrDataList.size());
        
        // 依部門代碼分組，避免重複處理
        Map<String, List<HRData>> departmentGroups = hrDataList.stream()
                .collect(Collectors.groupingBy(HRData::getDepCode));
        
        logger.info("需要處理的部門數量: {}", departmentGroups.size());
        
        for (Map.Entry<String, List<HRData>> entry : departmentGroups.entrySet()) {
            HRData sampleData = entry.getValue().get(0);
            try {
                processSingleDepartment(sampleData);
            } catch (Exception e) {
                logger.error("處理部門失敗: {} - {}", 
                    sampleData.getDepCode(), sampleData.getDepName(), e);
            }
        }
    }
    
    private void processSingleDepartment(HRData hrData) {
        String depName = hrData.getDepName();
        String depCode = hrData.getDepCode();
        
        if (depName == null || depName.trim().isEmpty()) {
            logger.warn("部門名稱為空，跳過處理。部門代碼: {}", depCode);
            return;
        }
        
        logger.debug("處理部門: {} ({})", depName, depCode);
        
        // 拆解部門名稱
        String[] deptParts = depName.split("-");
        
        // 處理每個層級的部門
        for (int i = 0; i < deptParts.length; i++) {
            String currentDeptName = deptParts[i].trim();
            String parentDeptCode = (i > 0) ? buildDeptCode(deptParts, i - 1) : null;
            String currentDeptCode = buildDeptCode(deptParts, i);
            int treeLevel = i + 1; // 從1開始
            
            // 檢查部門是否存在
            String checkSql = "SELECT COUNT(*) FROM CUS_HRImport_Department WHERE code = ?";
            Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, currentDeptCode);
            
            if (count == 0) {
                // 新增部門
                String insertSql = "INSERT INTO CUS_HRImport_Department " +
                        "(id, name, full_name, code, manager, parent_code, description, tree_level) " +
                        "VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?)";
                
                jdbcTemplate.update(insertSql, 
                        currentDeptName, currentDeptName, currentDeptCode, 
                        "系統管理員", parentDeptCode, currentDeptName, treeLevel);
                
                logger.info("新增部門: {} (代碼: {}, 層級: {})", currentDeptName, currentDeptCode, treeLevel);
            } else {
                // 更新部門
                String updateSql = "UPDATE CUS_HRImport_Department SET name = ?, full_name = ?, " +
                        "manager = ?, parent_code = ?, description = ?, tree_level = ? " +
                        "WHERE code = ?";
                
                jdbcTemplate.update(updateSql, 
                        currentDeptName, currentDeptName, "系統管理員", 
                        parentDeptCode, currentDeptName, treeLevel, currentDeptCode);
                
                logger.debug("更新部門: {} (代碼: {}, 層級: {})", currentDeptName, currentDeptCode, treeLevel);
            }
        }
    }
    
    private String buildDeptCode(String[] deptParts, int level) {
        StringBuilder code = new StringBuilder();
        for (int i = 0; i <= level; i++) {
            if (i > 0) code.append("-");
            code.append(deptParts[i].trim());
        }
        return code.toString();
    }
    
    private void processEmployees(List<HRData> hrDataList) {
        logger.info("開始處理員工資料，共 {} 筆記錄", hrDataList.size());
        
        int successCount = 0;
        int errorCount = 0;
        
        for (HRData hrData : hrDataList) {
            try {
                processSingleEmployee(hrData);
                successCount++;
            } catch (Exception e) {
                logger.error("處理員工資料失敗: {} ({})", 
                    hrData.getEmpName(), hrData.getWorkcard(), e);
                errorCount++;
            }
        }
        
        logger.info("員工資料處理完成: 成功 {} 筆, 失敗 {} 筆", successCount, errorCount);
    }
    
    private void processSingleEmployee(HRData hrData) {
        String workcard = hrData.getWorkcard();
        String empName = hrData.getEmpName();
        String mobile = hrData.getMobile();
        String depCode = hrData.getDepCode();
        
        if (workcard == null || workcard.trim().isEmpty()) {
            logger.warn("員工編號為空，跳過處理。員工姓名: {}", empName);
            return;
        }
        
        logger.debug("處理員工: {} ({})", empName, workcard);
        
        try {
            // 檢查員工帳號是否存在
            String checkAccountSql = "SELECT COUNT(*) FROM TsAccount WHERE FLoginName = ?";
            Integer accountCount = jdbcTemplate.queryForObject(checkAccountSql, Integer.class, workcard);
            
            // 取得部門ID
            String deptIdSql = "SELECT id FROM CUS_HRImport_Department WHERE code = ?";
            List<UUID> departmentIds = jdbcTemplate.queryForList(deptIdSql, UUID.class, depCode);
            UUID departmentId = departmentIds.isEmpty() ? null : departmentIds.get(0);
            
            if (departmentId == null) {
                logger.error("找不到對應的部門: {}", depCode);
                //throw new RuntimeException("部門不存在: " + depCode);
            }
            
            // 取得部門層級
            String levelSql = "SELECT tree_level FROM CUS_HRImport_Department WHERE code = ?";
            Integer treeLevel = jdbcTemplate.queryForObject(levelSql, Integer.class, depCode);
            
            if (accountCount == 0) {
                // 新增員工
                createNewEmployee(hrData, departmentId);
            } else {
                // 更新員工 - 根據層級決定是否更新部門
                updateEmployee(hrData, departmentId, treeLevel);
            }
            
        } catch (Exception e) {
            logger.error("處理員工資料失敗: {} ({})", empName, workcard, e);
            throw new RuntimeException("員工處理失敗", e);
        }
    }
    
    private void createNewEmployee(HRData hrData, UUID departmentId) {
        UUID employeeId = UUID.randomUUID();
        UUID accountId = UUID.randomUUID();
        UUID identityId = UUID.randomUUID();
        
        // 插入TsAccount
        String accountSql = "INSERT INTO TsAccount (FId, FName, FLoginName, FPassword, FEmail, FMobile, " +
                "FCreateTime, FEnabled, FLanguage) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), 1, 'zh-TW')";
        
        jdbcTemplate.update(accountSql, accountId, hrData.getEmpName(), hrData.getWorkcard(), 
                "default_password", "", hrData.getMobile());
        
        // 插入TsUser
        String userSql = "INSERT INTO TsUser (FId, FName, FLoginName, FPassword, FDepartmentId, " +
                "FMobile, FEmail, FEnabled, FOnGuard, FLanguage, FUserNo) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, 'zh-TW', ?)";
        
        jdbcTemplate.update(userSql, employeeId, hrData.getEmpName(), hrData.getWorkcard(), 
                "default_password", departmentId, hrData.getMobile(), "", hrData.getWorkcard());
        
        // 插入TsAccountIdentity
        String identitySql = "INSERT INTO TsAccountIdentity (FId, FName, FAccountId, FIdentityTypeId, " +
                "FEntityId, FDefault, FIndex) VALUES (?, ?, ?, NULL, ?, 1, 0)";
        
        jdbcTemplate.update(identitySql, identityId, hrData.getEmpName(), accountId, employeeId);
        
        logger.info("新增員工: {} ({})", hrData.getEmpName(), hrData.getWorkcard());
    }
    
    private void updateEmployee(HRData hrData, UUID departmentId, Integer treeLevel) {
        // 只有部門層級 < 4 時才更新部門
        String userUpdateSql;
        if (treeLevel != null && treeLevel < 4) {
            userUpdateSql = "UPDATE TsUser SET FName = ?, FMobile = ?, FDepartmentId = ? " +
                    "WHERE FLoginName = ?";
            jdbcTemplate.update(userUpdateSql, hrData.getEmpName(), hrData.getMobile(), 
                    departmentId, hrData.getWorkcard());
        } else {
            userUpdateSql = "UPDATE TsUser SET FName = ?, FMobile = ? WHERE FLoginName = ?";
            jdbcTemplate.update(userUpdateSql, hrData.getEmpName(), hrData.getMobile(), 
                    hrData.getWorkcard());
        }
        
        // 更新TsAccount
        String accountUpdateSql = "UPDATE TsAccount SET FName = ?, FMobile = ? WHERE FLoginName = ?";
        jdbcTemplate.update(accountUpdateSql, hrData.getEmpName(), hrData.getMobile(), 
                hrData.getWorkcard());
        
        logger.debug("更新員工: {} ({})", hrData.getEmpName(), hrData.getWorkcard());
    }
}