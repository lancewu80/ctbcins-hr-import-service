package com.ctbcins.hrimport.service;

import com.ctbcins.hrimport.dto.HRData;
import com.ctbcins.hrimport.entity.Department;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.input.BOMInputStream;
import java.io.*;

import org.springframework.dao.EmptyResultDataAccessException;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.nio.file.Files;

@Service
public class HRDataProcessService {
    private static final Logger logger = LoggerFactory.getLogger(HRDataProcessService.class);
    
    @Autowired
    private EntityManager entityManager;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Value("${app.processing.enabled-states:A}")
    private String enabledStates;

    @Value("${scheduler.hrimport.localArchivePath:}")
    private String localArchivePath;

    private static final DateTimeFormatter CSV_DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Transactional
    public void processHRFile(String filePath) {
        logger.info("開始處理HR檔案: {}", filePath);

        boolean archiveRequested = false;
        try (InputStream inputStream = new FileInputStream(filePath);
             BOMInputStream bomInputStream = new BOMInputStream(inputStream);
             Reader reader = new InputStreamReader(bomInputStream, StandardCharsets.UTF_8)) {

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


            // 1) 先把原始讀到的資料寫入 CUS_HrImport（包含檔名與建立時間）
            insertRawImportRecords(hrDataList, filePath);

            // request archive after we finish parsing and inserting raw records
            archiveRequested = true;

            // 2) 再過濾有效資料繼續後續處理（原有邏輯）

            List<HRData> validData = hrDataList.stream()
                    .filter(data -> data.getDepCode() != null && !data.getDepCode().trim().isEmpty())
                    .filter(data -> data.getEmpName() != null && !data.getEmpName().trim().isEmpty())
                    .filter(data -> enabledStates.contains(data.getStateNo()))
                    .collect(Collectors.toList());
            
            logger.info("CSV檔案解析完成，總記錄數: {}, 有效記錄數: {}", 
                hrDataList.size(), validData.size());
            
            if (validData.isEmpty()) {
                logger.warn("沒有有效的HR資料需要處理");
                // do not return here; allow archive to run after resources are closed
            } else {
                // 處理部門資料
                processDepartments(validData);

                // 處理員工資料
                processEmployees(validData);

                logger.info("成功處理HR資料檔案: {}, 有效記錄數: {}", filePath, validData.size());
            }

         } catch (Exception e) {
             logger.error("處理HR檔案失敗: {}", filePath, e);
             throw new RuntimeException("HR檔案處理失敗", e);
         }

         // Ensure file resources are closed before attempting filesystem operations.
        if (archiveRequested) {
            try {
                copyThenDeleteToArchive(filePath);
            } catch (Exception ex) {
                logger.warn("將檔案 copy->delete 到 archive 時發生錯誤（不重試）: {}", filePath, ex);
            }
        }
    }


    /**
     * 將解析到的原始 CSV 資料批次寫入 CUS_HRImport
     */
    private void insertRawImportRecords(List<HRData> hrDataList, String filePath) {
        if (hrDataList == null || hrDataList.isEmpty()) {
            logger.info("無需寫入 CUS_HRImport，資料為空");
            return;
        }

        final String fileName = Paths.get(filePath).getFileName().toString();

        final String insertSql = "INSERT INTO CUS_HRImport " +
                "(id, cpnyid, dep_no, dep_code, dep_name, state_no, state_name, emp_id, emp_name, workcard, inadate, quitdate, stop_w, start_w, mdate, position_name, mobile, title_name, workplace_name, file_name) " +
                "VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    HRData d = hrDataList.get(i);
                    ps.setString(1, safeTrim(d.getCpnyId()));
                    ps.setString(2, safeTrim(d.getDepNo()));
                    ps.setString(3, safeTrim(d.getDepCode()));
                    ps.setString(4, safeTrim(d.getDepName()));
                    ps.setString(5, safeTrim(d.getStateNo()));
                    ps.setString(6, safeTrim(d.getStateName()));
                    ps.setString(7, safeTrim(d.getEmpId()));
                    ps.setString(8, safeTrim(d.getEmpName()));
                    ps.setString(9, safeTrim(d.getWorkcard()));

                    // inadate
                    java.sql.Date ina = parseCsvDateToSqlDate(d.getInaDate());
                    if (ina != null) ps.setDate(10, ina); else ps.setNull(10, java.sql.Types.DATE);

                    // quitdate
                    java.sql.Date qd = parseCsvDateToSqlDate(d.getQuitDate());
                    if (qd != null) ps.setDate(11, qd); else ps.setNull(11, java.sql.Types.DATE);

                    ps.setString(12, safeTrim(d.getStopW()));
                    ps.setString(13, safeTrim(d.getStartW()));

                    // mdate
                    java.sql.Date md = parseCsvDateToSqlDate(d.getMdate());
                    if (md != null) ps.setDate(14, md); else ps.setNull(14, java.sql.Types.DATE);

                    ps.setString(15, safeTrim(d.getPositionName()));
                    ps.setString(16, safeTrim(d.getMobile()));
                    ps.setString(17, safeTrim(d.getTitleName()));
                    ps.setString(18, safeTrim(d.getWorkplaceName()));

                    ps.setString(19, fileName);
                }

                @Override
                public int getBatchSize() {
                    return hrDataList.size();
                }
            });

            logger.info("已將 {} 筆原始資料匯入 CUS_HRImport (file={})", hrDataList.size(), fileName);
        } catch (Exception ex) {
            logger.error("寫入 CUS_HRImport 發生錯誤", ex);
            throw new RuntimeException("CUS_HRImport 寫入失敗", ex);
        }
    }

    private String safeTrim(String s) {
        return s == null ? null : s.trim();
    }

    private java.sql.Date parseCsvDateToSqlDate(String s) {
        if (s == null) return null;
        String trimmed = s.trim();
        if (trimmed.isEmpty()) return null;
        try {
            // CSV 格式: yyyyMMdd
            LocalDate ld = LocalDate.parse(trimmed, CSV_DATE_FMT);
            return Date.valueOf(ld);
        } catch (DateTimeParseException e) {
            logger.debug("無法解析日期字串: {}", s);
            return null;
        }
    }

    private void processDepartments(List<HRData> hrDataList) {
        logger.info("開始處理部門資料，共 {} 筆記錄", hrDataList.size());

        // Build unique set of full DEP_NAME values from CSV (avoid creating parent-only entries)
        Map<String, HRData> fullDeptMap = hrDataList.stream()
                .filter(d -> d.getDepName() != null && !d.getDepName().trim().isEmpty())
                .collect(Collectors.toMap(HRData::getDepName, d -> d, (a, b) -> a));

        logger.info("需要處理的部門數量 (依完整 DEP_NAME): {}", fullDeptMap.size());

        for (Map.Entry<String, HRData> entry : fullDeptMap.entrySet()) {
            HRData sampleData = entry.getValue();
            try {
                processSingleDepartment(sampleData);
            } catch (Exception e) {
                logger.error("處理部門失敗: {} - {}", 
                    sampleData.getDepCode(), sampleData.getDepName(), e);
            }
        }
    }
    
    private void processSingleDepartment(HRData hrData) {
        String fullDeptName = hrData.getDepName();
        String depCode = hrData.getDepCode();
        String depNo = hrData.getDepNo();
        String cpnyId = hrData.getCpnyId();

        if (fullDeptName == null || fullDeptName.trim().isEmpty()) {
            logger.warn("部門名稱為空，跳過處理。部門代碼: {}", depCode);
            return;
        }

        // compute parts
        String[] parts = fullDeptName.split("-");
        String shortName = parts[parts.length - 1].trim();
        String parentDeptCode = (parts.length > 1) ? String.join("-", Arrays.copyOf(parts, parts.length - 1)) : null;
        String code = fullDeptName.trim(); // keep full dept string as unique code
        int treeLevel = parts.length + 1; // preserve previous convention

        logger.debug("處理部門(完整名稱): {} -> 短名: {}, 代碼: {}, 父代碼: {}, 層級: {}", fullDeptName, shortName, code, parentDeptCode, treeLevel);

        // 檢查部門是否存在（以 code 為唯一鍵）
        String checkSql = "SELECT COUNT(*) FROM CUS_HRImport_Department WHERE code = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, code);

        if (count == null || count == 0) {
            // 新增部門 (name = shortName, full_name = fullDeptName)
            String insertSql = "INSERT INTO CUS_HRImport_Department " +
                    "(id, cpynid, dep_no, dep_code, name, full_name, code, manager, parent_code, description, tree_level) " +
                    "VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            jdbcTemplate.update(insertSql,
                    cpnyId, depNo, depCode, shortName, fullDeptName, code,
                    "系統管理員", parentDeptCode, depNo, treeLevel);

            logger.info("新增部門: {} (代碼: {}, 層級: {})", fullDeptName, code, treeLevel);
        } else {
            // 更新部門
            String updateSql = "UPDATE CUS_HRImport_Department SET cpynid = ?, dep_no = ?, dep_code = ?, name = ?, full_name = ?, " +
                    "manager = ?, parent_code = ?, description = ?, tree_level = ? " +
                    "WHERE code = ?";

            jdbcTemplate.update(updateSql,
                    cpnyId, depNo, depCode, shortName, fullDeptName, "系統管理員",
                    parentDeptCode, depNo, treeLevel, code);

            logger.debug("更新部門: {} (代碼: {}, 層級: {})", fullDeptName, code, treeLevel);
        }

        // 取得或查詢剛剛 insert/update 的 CUS_HRImport_Department.id
        UUID cusId = null;
        try {
            String idSql = "SELECT id FROM CUS_HRImport_Department WHERE code = ?";
            cusId = jdbcTemplate.queryForObject(idSql, UUID.class, code);
        } catch (EmptyResultDataAccessException e) {
            logger.warn("未取得 CUS_HRImport_Department id (code={})", code);
        }

        if (cusId != null) {
            Department dept = new Department();
            dept.setId(cusId);
            dept.setCpynid(cpnyId);
            dept.setDep_no(depNo);
            dept.setDep_code(depCode);
            dept.setName(shortName);
            dept.setFullName(fullDeptName);
            dept.setCode(code);
            dept.setManager("系統管理員");
            dept.setParentCode(parentDeptCode);
            dept.setDescription(depNo);
            dept.setTreeLevel(treeLevel);

            try {
                insertOrUpdateTsDepartment(dept);
            } catch (Exception e) {
                logger.error("同步到 TsDepartment 失敗 (code={}): {}", code, e.getMessage(), e);
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

    /**
     * 插入或更新 TsDepartment 紀錄
     * @param dept 由 CUS_HRImport_Department entity 提供的部門資訊
     */
    private void insertOrUpdateTsDepartment(Department dept) {

        // Step 1: 查找 FParentId (父部門的 FId)
        UUID parentFId = null;
        if (dept.getTreeLevel() != null && dept.getTreeLevel() == 2) {
            // tree level 2 => top-level department, map to provided root GUID
            parentFId = UUID.fromString("00000000-0000-0000-1001-000000000001");
        } else if (dept.getParentCode() != null && !dept.getParentCode().isEmpty()) {
            try {
                String parentIdSql = "SELECT id FROM CUS_HRImport_Department WHERE code = ?";
                parentFId = jdbcTemplate.queryForObject(parentIdSql, UUID.class, dept.getParentCode());
            } catch (EmptyResultDataAccessException e) {
                logger.warn("未找到父部門 (code={}) 的 CUS id，FParentId 設為 NULL。", dept.getParentCode());
                parentFId = null;
            } catch (Exception e) {
                logger.error("查找父部門FId時發生錯誤，Code: {}", dept.getParentCode(), e);
            }
        }

        // Step 2: 使用 MERGE INTO 插入或更新 TsDepartment
        // TsDepartment 欄位: FId, FParentId, FIndex, FTreeLevel, FTreeSerial, FName, FFullName, FShortCode, FDescription

        String mergeSql =
                "MERGE INTO TsDepartment AS Target " +
                        "USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) " +
                        "AS Source (FId, FParentId, FIndex, FTreeLevel, FName, FFullName, FShortCode, FDescription, FTreeSerial) " +
                        "ON (Target.FId = Source.FId) " +
                        "WHEN MATCHED THEN " +
                        "    UPDATE SET " +
                        "        FParentId = Source.FParentId, " +
                        "        FTreeLevel = Source.FTreeLevel, " +
                        "        FName = Source.FName, " +
                        "        FFullName = Source.FFullName, " +
                        "        FShortCode = Source.FShortCode, " +
                        "        FDescription = Source.FDescription, " +
                        "        FTreeSerial = Source.FTreeSerial, " +
                        "        FUserId = ? , " +
                        "        FIsServices = 0, " +
                        "        FIsSales = 0, " +
                        "        FEnabled = 1, " +
                        "        FIsCompany = 0 " +
                        "WHEN NOT MATCHED BY TARGET THEN " +
                        "    INSERT (FId, FParentId, FIndex, FTreeLevel, FTreeSerial, FName, FFullName, FShortCode, FDescription, FUserId, FEnabled, FIsCompany, FIsServices, FIsSales) " +
                        "    VALUES (Source.FId, Source.FParentId, Source.FIndex, Source.FTreeLevel, Source.FTreeSerial, Source.FName, Source.FFullName, Source.FShortCode, Source.FDescription, ?, 1, 0, 0, 0);";

        UUID fixedUserId = UUID.fromString("00000000-0000-0000-1002-000000000001");

        // 執行 MERGE 語句
        jdbcTemplate.update(mergeSql,
                dept.getId(),                 // FId
                parentFId,                    // FParentId
                0,                             // FIndex (預設 0)
                dept.getTreeLevel() != null ? dept.getTreeLevel() : 1, // FTreeLevel
                dept.getName(),               // FName
                dept.getFullName(),           // FFullName
                dept.getDep_code(),           // FShortCode (mapped from dep_code)
                dept.getDescription(),        // FDescription
                dept.getCode(),               // FTreeSerial (使用 Code)
                fixedUserId,                  // set FUserId on update
                fixedUserId                   // set FUserId on insert
        );

        logger.info("TsDepartment 同步: {} ({}) 完成, FParentId: {}", dept.getName(), dept.getCode(), parentFId);
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
        String depName = hrData.getDepName();

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
            String deptIdSql = "SELECT id FROM CUS_HRImport_Department WHERE dep_code = ?";
            List<UUID> departmentIds = jdbcTemplate.queryForList(deptIdSql, UUID.class, depCode);
            UUID departmentId = departmentIds.isEmpty() ? null : departmentIds.get(0);
            
            if (departmentId == null) {
                logger.error("找不到對應的部門: {}", depCode);
                //throw new RuntimeException("部門不存在: " + depCode);
            }
            
            // 取得部門層級
            String levelSql = "SELECT tree_level FROM CUS_HRImport_Department WHERE dep_code = ?";
            Integer treeLevel = null;
            try {
                treeLevel = jdbcTemplate.queryForObject(levelSql, Integer.class, depCode);
            } catch (EmptyResultDataAccessException ex) {
                logger.warn("查不到部門層級 (code={})，預設為NULL", depCode);
            }

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

    /**
     * Copy the file to archive with timestamped name, then attempt to delete original once.
     * No retries. If copy succeeds and delete fails, original is left in place and we log a warning.
     */
    private void copyThenDeleteToArchive(String filePath) {
        Path source = Paths.get(filePath);

        // determine archive dir from Spring config if available
        Path archiveDir = (localArchivePath != null && !localArchivePath.isEmpty())
                ? Paths.get(localArchivePath)
                : Paths.get("C:", "project", "testdata", "archive");

        try {
            if (!Files.exists(archiveDir)) {
                Files.createDirectories(archiveDir);
                logger.info("建立 archive 資料夾: {}", archiveDir.toAbsolutePath());
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            String originalName = source.getFileName().toString();
            Path target = archiveDir.resolve(timestamp + "-" + originalName);

            // copy file (one attempt)
            Files.copy(source, target, REPLACE_EXISTING);
            logger.info("已複製檔案到 archive: {} -> {}", source.toAbsolutePath(), target.toAbsolutePath());

            // attempt to delete original (one attempt, no retry)
            try {
                Files.delete(source);
                logger.info("已刪除原始檔案: {}", source.toAbsolutePath());
            } catch (IOException delEx) {
                logger.warn("無法刪除原始檔案，保留原檔: {}，原因: {}", source.toAbsolutePath(), delEx.getMessage());
            }

        } catch (IOException e) {
            logger.warn("將檔案複製到 archive 時發生錯誤（不重試）: {}", filePath, e);
        }
    }
}

