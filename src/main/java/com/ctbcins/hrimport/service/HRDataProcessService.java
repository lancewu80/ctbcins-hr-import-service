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

    @Value("${app.default-password:no_default_password}")
    private String defaultPassword;

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

            // derive filename for error logging
            final String sourceFileName = Paths.get(filePath).getFileName().toString();

            // 2) 再過濾有效資料繼續後續處理（原有邏輯）

            List<HRData> validData = hrDataList.stream()
                    .filter(data -> data.getDepCode() != null && !data.getDepCode().trim().isEmpty())
                    .filter(data -> data.getEmpName() != null && !data.getEmpName().trim().isEmpty())
                    .filter(data -> enabledStates.contains(data.getStateNo()))
                    // sort by number of '-' in depName (no '-' => 0, come first), then by depName string
                    .sorted(Comparator
                            .comparingInt((HRData h) -> countDashes(h.getDepName()))
                            .thenComparing(h -> Optional.ofNullable(h.getDepName()).orElse("")))
                    .collect(Collectors.toList());
            
            logger.info("CSV檔案解析完成，總記錄數: {}, 有效記錄數: {}", 
                hrDataList.size(), validData.size());
            
            if (validData.isEmpty()) {
                logger.warn("沒有有效的HR資料需要處理");
                // do not return here; allow archive to run after resources are closed
            } else {
                // 處理部門資料
                processDepartments(validData, sourceFileName);

                // 處理員工資料
                processEmployees(validData, sourceFileName);

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
            // persist raw import error to error log (so we know which file failed)
            try {
                final String fileNameEx = Paths.get(filePath).getFileName().toString();
                insertErrorLog("RAW", null, null, ex, fileNameEx);
            } catch (Exception ignore) {
                logger.warn("寫入 CUS_HRImport_Error_Log 失敗（原始錯誤）: {}", ignore.getMessage());
            }
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

    private void processDepartments(List<HRData> hrDataList, String sourceFileName) {
        logger.info("開始處理部門資料，共 {} 筆記錄", hrDataList.size());

        // Ensure three predefined level-2 parent departments exist (they are not present in CSV)
        ensureDefaultParentDepartments(sourceFileName);

        // Build unique list of full DEP_NAME values from CSV (avoid creating parent-only entries)
        List<HRData> uniqueDeptList = hrDataList.stream()
                .filter(d -> d.getDepName() != null && !d.getDepName().trim().isEmpty())
                .collect(Collectors.toMap(HRData::getDepName, d -> d, (a, b) -> a))
                .values().stream()
                // sort by hierarchy depth: fewer '-' means higher level (parents first), then by name
                .sorted(Comparator
                        .comparingInt((HRData h) -> countDashes(h.getDepName()))
                        .thenComparing(h -> Optional.ofNullable(h.getDepName()).orElse("")))
                .collect(Collectors.toList());

        logger.info("需要處理的部門數量 (依完整 DEP_NAME): {}", uniqueDeptList.size());

        for (HRData sampleData : uniqueDeptList) {
            try {
                processSingleDepartment(sampleData, sourceFileName);
            } catch (Exception e) {
                logger.error("處理部門失敗: {} - {}", sampleData.getDepCode(), sampleData.getDepName(), e);
                // persist error log
                insertErrorLog("DEPARTMENT", sampleData.getDepCode(), sampleData, e, sourceFileName);
            }
        }
    }
    
    private void processSingleDepartment(HRData hrData, String sourceFileName) {
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
         // Special mapping: when tree level is 3, map certain '直轄' prefixes to simplified parents
         int treeLevel = parts.length + 1; // preserve previous convention
         String normalized = fullDeptName.trim();
         if (treeLevel == 3) {
             if (normalized.startsWith("總經理直轄-")) {
                 parentDeptCode = "總經理";
             } else if (normalized.startsWith("董事長直轄-")) {
                 parentDeptCode = "董事長";
             }
         }
         // Ensure the parent chain exists in CUS_HRImport_Department before inserting this department
         if (parentDeptCode != null && !parentDeptCode.trim().isEmpty()) {
             try {
                 ensureParentChain(parentDeptCode, sourceFileName);
             } catch (Exception e) {
                 logger.warn("建立或確認父部門鏈失敗 (parentCode={}): {}", parentDeptCode, e.getMessage(), e);
                 insertErrorLog("DEPARTMENT", depCode, hrData, e, sourceFileName);
             }
         }
         String code = fullDeptName.trim(); // keep full dept string as unique code

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
                // log error into error table with department details
                insertErrorLog("DEPARTMENT", depCode, hrData, e, sourceFileName);
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

    private void processEmployees(List<HRData> hrDataList, String sourceFileName) {
        logger.info("開始處理員工資料，共 {} 筆記錄", hrDataList.size());
        
        int successCount = 0;
        int errorCount = 0;
        
        for (HRData hrData : hrDataList) {
            try {
                processSingleEmployee(hrData, sourceFileName);
                successCount++;
            } catch (Exception e) {
                logger.error("處理員工資料失敗: {} ({})", 
                    hrData.getEmpName(), hrData.getWorkcard(), e);
                errorCount++;
                // insert error log with employee details
                insertErrorLog("EMPLOYEE", hrData.getWorkcard(), hrData, e, sourceFileName);
            }
        }
        
        logger.info("員工資料處理完成: 成功 {} 筆, 失敗 {} 筆", successCount, errorCount);
    }
    
    private void processSingleEmployee(HRData hrData, String sourceFileName) {
        String workcard = hrData.getWorkcard();
        String empName = hrData.getEmpName();
        String mobile = hrData.getMobile();
        String depCode = hrData.getDepCode();
        String depName = hrData.getDepName();
        String depNo = hrData.getDepNo();

        if (workcard == null || workcard.trim().isEmpty()) {
            logger.warn("員工編號為空，跳過處理。員工姓名: {}", empName);
            return;
        }
        
        logger.debug("處理員工: {} ({})", empName, workcard);
        
        try {
            // 檢查員工帳號是否存在
            String checkAccountSql = "SELECT COUNT(*) FROM TsAccount WHERE FLoginName = ?";
            Integer accountCount = jdbcTemplate.queryForObject(checkAccountSql, Integer.class, workcard);
            
            // 取得部門ID — use dep_no to avoid ambiguity when multiple departments share the same dep_code
            String deptIdSql = "SELECT id FROM CUS_HRImport_Department WHERE dep_no = ?";
            List<UUID> departmentIds = jdbcTemplate.queryForList(deptIdSql, UUID.class, depNo);
            UUID departmentId = null;
            if (departmentIds == null || departmentIds.isEmpty()) {
                departmentId = null;
            } else {
                departmentId = departmentIds.get(0);
                if (departmentIds.size() > 1) {
                    logger.warn("dep_no={} 對應多筆 CUS_HRImport_Department id，使用第一筆 id={}", depNo, departmentId);
                    insertErrorLog("EMPLOYEE", workcard, hrData,
                            new RuntimeException("Multiple CUS_HRImport_Department rows for dep_no=" + depNo),
                            sourceFileName);
                }
            }

            if (departmentId == null) {
                logger.error("找不到對應的部門 dep_no={}", depNo);
                // record error; optionally continue or throw depending on business needs
                insertErrorLog("EMPLOYEE", workcard, hrData,
                        new RuntimeException("Department not found for dep_no=" + depNo),
                        sourceFileName);
            }

            // 取得部門層級，使用 dep_no 作為查詢鍵
            String levelSql = "SELECT tree_level FROM CUS_HRImport_Department WHERE dep_no = ?";
            Integer treeLevel = null;
            try {
                List<Integer> levels = jdbcTemplate.queryForList(levelSql, Integer.class, depNo);
                if (levels == null || levels.isEmpty()) {
                    treeLevel = null;
                } else {
                    if (levels.size() > 1) {
                        logger.warn("dep_no={} 對應多筆 tree_level，使用第一筆: {}", depNo, levels.get(0));
                        insertErrorLog("EMPLOYEE", workcard, hrData,
                                new RuntimeException("Multiple tree_level rows for dep_no=" + depNo),
                                sourceFileName);
                    }
                    treeLevel = levels.get(0);
                }
            } catch (Exception ex) {
                logger.warn("查詢部門層級時發生例外 (dep_no={})，預設為NULL: {}", depNo, ex.getMessage());
                treeLevel = null;
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
            // log error with file name
            insertErrorLog("EMPLOYEE", workcard, hrData, e, sourceFileName);
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
                defaultPassword, "", hrData.getMobile());

        // 插入TsUser
        String userSql = "INSERT INTO TsUser (FId, FName, FLoginName, FPassword, FDepartmentId, " +
                "FMobile, FEmail, FEnabled, FOnGuard, FLanguage, FUserNo) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, 'zh-TW', ?)";
        
        jdbcTemplate.update(userSql, employeeId, hrData.getEmpName(), hrData.getWorkcard(), 
                defaultPassword, departmentId, hrData.getMobile(), "", hrData.getWorkcard());

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

    private static int countDashes(String s) {
        if (s == null) return 0;
        String trimmed = s.trim();
        if (trimmed.isEmpty()) return 0;
        int cnt = 0;
        for (int i = 0; i < trimmed.length(); i++) {
            if (trimmed.charAt(i) == '-') cnt++;
        }
        return cnt;
    }

    /**
     * Insert an error record into CUS_HRImport_Error_Log
     * @param recordType DEPARTMENT | EMPLOYEE | RAW
     * @param recordKey dep_code or workcard
     * @param hrData the HRData object to persist (will be stringified)
     * @param ex the exception
     * @param fileName optional source file name
     */
    private void insertErrorLog(String recordType, String recordKey, HRData hrData, Exception ex, String fileName) {
        try {
            // store processed_at as Taipei local time (UTC+8) regardless of DB server timezone
            String insertSql = "INSERT INTO CUS_HRImport_Error_Log (id, file_name, record_type, record_key, payload, error_message, stack_trace, processed_at, created_by) " +
                    "VALUES (NEWID(), ?, ?, ?, ?, ?, ?, CONVERT(datetime2, SWITCHOFFSET(SYSUTCDATETIME() AT TIME ZONE 'UTC', '+08:00')), ?)";

            String payload = hrData == null ? null : hrData.toString();
            String errMsg = ex == null ? "" : ex.getMessage();
            String stack = null;
            if (ex != null) {
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                stack = sw.toString();
            }

            jdbcTemplate.update(insertSql,
                    fileName,              // file_name
                    recordType,            // record_type
                    recordKey,             // record_key
                    payload,               // payload
                    errMsg,                // error_message
                    stack,                 // stack_trace
                    "hr-import-service"   // created_by
            );
        } catch (Exception logEx) {
            logger.error("寫入 CUS_HRImport_Error_Log 失敗: {}", logEx.getMessage(), logEx);
        }
    }

    /**
     * Ensure predefined parent departments exist in CUS_HRImport_Department and TsDepartment.
     * Adds entries required so CSV children can resolve their parent departments.
     */
    private void ensureDefaultParentDepartments(String sourceFileName) {
        // Entries: {code, parentCode, treeLevel}
        final String[][] defs = new String[][]{
                {"總經理", null, "2"},
                {"營運規劃處", null, "2"},
                {"董事長", null, "2"},
                // intermediate department under 總經理 so children like
                // '總經理直轄-法令遵循部-法令遵循二科' can find parent '總經理直轄-法令遵循部'
                {"總經理直轄-法令遵循部", "總經理", "3"}
        };

        for (String[] def : defs) {
            final String code = def[0].trim();
            final String parentCode = def[1] == null ? null : def[1].trim();
            final int treeLevel = Integer.parseInt(def[2]);
            try {
                String checkSql = "SELECT COUNT(*) FROM CUS_HRImport_Department WHERE code = ?";
                Integer cnt = jdbcTemplate.queryForObject(checkSql, Integer.class, code);
                if (cnt == null || cnt == 0) {
                    String insertSql = "INSERT INTO CUS_HRImport_Department " +
                            "(id, cpynid, dep_no, dep_code, name, full_name, code, manager, parent_code, description, tree_level) " +
                            "VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    jdbcTemplate.update(insertSql,
                            null, // cpynid
                            null, // dep_no
                            null, // dep_code
                            code, // name
                            code, // full_name
                            code, // code
                            "系統管理員", // manager
                            parentCode, // parent_code
                            null, // description
                            treeLevel // tree_level
                    );
                    logger.info("已建立預設部門 (CUS): {} (parent={})", code, parentCode);

                    // fetch id and sync to TsDepartment
                    try {
                        UUID cusId = jdbcTemplate.queryForObject("SELECT id FROM CUS_HRImport_Department WHERE code = ?", UUID.class, code);
                        if (cusId != null) {
                            Department dept = new Department();
                            dept.setId(cusId);
                            dept.setCpynid(null);
                            dept.setDep_no(null);
                            dept.setDep_code(null);
                            dept.setName(code);
                            dept.setFullName(code);
                            dept.setCode(code);
                            dept.setManager("系統管理員");
                            dept.setParentCode(parentCode);
                            dept.setDescription(null);
                            dept.setTreeLevel(treeLevel);
                            try {
                                insertOrUpdateTsDepartment(dept);
                                logger.info("已同步預設部門到 TsDepartment: {}", code);
                            } catch (Exception e) {
                                logger.warn("同步預設部門到 TsDepartment 失敗: {}", code, e);
                                insertErrorLog("DEPARTMENT", code, null, e, sourceFileName);
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("建立後查詢預設部門 id 失敗: {}", code, e);
                        insertErrorLog("DEPARTMENT", code, null, e, sourceFileName);
                    }
                }
            } catch (Exception e) {
                logger.error("檢查或建立預設部門失敗: {}", code, e);
                insertErrorLog("DEPARTMENT", code, null, e, sourceFileName);
            }
        }
    }

    /**
     * Ensure the parent department chain exists in CUS_HRImport_Department.
     * If a parent (by full code) is missing, this will recursively create its parent first,
     * then insert the missing parent and synchronize to TsDepartment.
     */
    private void ensureParentChain(String parentCode, String sourceFileName) {
        if (parentCode == null || parentCode.trim().isEmpty()) return;
        String code = parentCode.trim();
        try {
            Integer cnt = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM CUS_HRImport_Department WHERE code = ?", Integer.class, code);
            if (cnt != null && cnt > 0) return; // already exists
        } catch (Exception e) {
            logger.warn("檢查父部門是否存在時發生例外: {}", e.getMessage(), e);
        }

        // determine parent-of-parent
        String[] parts = code.split("-");
        String parentOfParent = (parts.length > 1) ? String.join("-", Arrays.copyOf(parts, parts.length - 1)) : null;
        // recursively ensure higher-level parent exists first
        if (parentOfParent != null && !parentOfParent.trim().isEmpty()) {
            ensureParentChain(parentOfParent, sourceFileName);
        }

        // compute tree level and short name
        int treeLevel = parts.length + 1;
        String shortName = parts[parts.length - 1].trim();

        // insert parent record into CUS_HRImport_Department
        try {
            String insertSql = "INSERT INTO CUS_HRImport_Department " +
                    "(id, cpynid, dep_no, dep_code, name, full_name, code, manager, parent_code, description, tree_level) " +
                    "VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            jdbcTemplate.update(insertSql,
                    null, // cpynid
                    null, // dep_no
                    null, // dep_code
                    shortName, // name
                    code, // full_name
                    code, // code
                    "系統管理員", // manager
                    parentOfParent, // parent_code
                    null, // description
                    treeLevel // tree_level
            );
            logger.info("自動建立父部門: {} (parent={})", code, parentOfParent);

            // sync to TsDepartment
            try {
                UUID cusId = jdbcTemplate.queryForObject("SELECT id FROM CUS_HRImport_Department WHERE code = ?", UUID.class, code);
                if (cusId != null) {
                    Department dept = new Department();
                    dept.setId(cusId);
                    dept.setCpynid(null);
                    dept.setDep_no(null);
                    dept.setDep_code(null);
                    dept.setName(shortName);
                    dept.setFullName(code);
                    dept.setCode(code);
                    dept.setManager("系統管理員");
                    dept.setParentCode(parentOfParent);
                    dept.setDescription(null);
                    dept.setTreeLevel(treeLevel);
                    insertOrUpdateTsDepartment(dept);
                }
            } catch (Exception e) {
                logger.warn("自動建立父部門後同步 TsDepartment 失敗: {}", code, e);
                insertErrorLog("DEPARTMENT", code, null, e, sourceFileName);
            }
        } catch (Exception e) {
            logger.warn("建立父部門失敗: {}", code, e);
            insertErrorLog("DEPARTMENT", code, null, e, sourceFileName);
        }
    }
}
