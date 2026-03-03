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

    @Autowired
    private ErrorLogService errorLogService;

    @Value("${app.processing.enabled-states:A}")
    private String enabledStates;

    @Value("${scheduler.hrimport.localArchivePath:}")
    private String localArchivePath;

    @Value("${app.default-password:no_default_password}")
    private String defaultPassword;

    @Value("${app.ts.start-ftreeserial:001.001}")
    private String startFTreeSerial;

    // Top-level/root department FId (used as FParentId for tree level 2 departments) - configurable in application.yml
    @Value("${app.ts.root-fid:00000000-0000-0000-1001-000000000001}")
    private String tsRootFid;

    // Dept-level fixed user id for TsDepartment FUserId column; configurable in application.yml
    @Value("${app.ts.dept-user-id:00000000-0000-0000-1002-000000000001}")
    private String tsDeptUserId;

    // identity-type-id now structured in application.yml as a map with 'id' and 'rol_user_id'
    @Value("${app.identity-type-id.id:564CF69E-76D6-4BAF-B584-6E04C2911DAE}")
    private String defaultIdentityTypeId;

    // 新增：設定從 application.yml 讀取用於 TsRoleUser 的角色 ID（放在 app.identity-type-id.rol_user_id）
    @Value("${app.identity-type-id.rol_user_id:}")
    private String tsRoleUserRoleId;

    // 新增：可在 application.yml 或 scheduler.hrimport.filename 設定要處理的 HR input 檔名（預設 HrImport.csv）
    @Value("${scheduler.hrimport.filename:HrImport.csv}")
    private String hrImportFileName;

    // 可在 application.yml 設定: app.tree-label-update-dep，預設值為 4
    @Value("${app.tree-label-update-dep:4}")
    private Integer treeLabelUpdateDep;

    private static final DateTimeFormatter CSV_DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Transactional
    public void processHRFile(String filePath) {
        logger.info("開始處理HR檔案: {}", filePath);

        // Only process files that match configured hrImportFileName (if set)
        String sourceFileName = Paths.get(filePath).getFileName().toString();
        if (hrImportFileName != null && !hrImportFileName.trim().isEmpty() && !sourceFileName.equals(hrImportFileName.trim())) {
            logger.info("檔案 {} 非目標 HR import 檔名 (設定: {}), 跳過處理", sourceFileName, hrImportFileName);
            return;
        }

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
            // final String sourceFileName = Paths.get(filePath).getFileName().toString();

            // 2) 再過濾有效資料繼續後續處理（原有邏輯）

            List<HRData> validData = hrDataList.stream()
                    .filter(data -> data.getDepCode() != null && !data.getDepCode().trim().isEmpty())
                    .filter(data -> data.getEmpName() != null && !data.getEmpName().trim().isEmpty())
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

        final String insertSql = "INSERT INTO public.\"CUS_HRImport\" " +
                "(cpnyid, dep_no, dep_code, dep_name, state_no, state_name, emp_id, emp_name, workcard, inadate, quitdate, stop_w, start_w, mdate, position_name, mobile, title_name, workplace_name, file_name) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
                logger.warn("寫入 CUS_HrImport_Error_Log 失敗（原始錯誤）: {}", ignore.getMessage());
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

         // Build a unique map of full DEP_NAME values from CSV. Also include special workplace-derived
         // level-6 entries when DEP_NAME contains 商品處-理賠部-理賠一科- and WORKPLACE_NAME != '總公司'.
         Map<String, HRData> uniqueMap = new LinkedHashMap<>();
         for (HRData d : hrDataList) {
             if (d.getDepName() == null || d.getDepName().trim().isEmpty()) continue;
             String key = d.getDepName().trim();
             if (!uniqueMap.containsKey(key)) uniqueMap.put(key, d);
         }

         // Add workplace-specific (level-6) department entries so they are created/synced before employees
         for (HRData d : hrDataList) {
             try {
                 String dep = d.getDepName();
                 String wp = d.getWorkplaceName();
                 if (dep != null && dep.contains("商品處-理賠部-理賠一科-") && wp != null && !wp.trim().isEmpty() && !"總公司".equals(wp.trim())) {
                     String newFull = dep.trim() + "-" + wp.trim();
                     if (!uniqueMap.containsKey(newFull)) {
                         HRData copy = new HRData();
                         copy.setCpnyId(d.getCpnyId());
                         copy.setDepNo(d.getDepNo());
                         copy.setDepCode(d.getDepCode());
                         copy.setDepName(newFull);
                         copy.setWorkplaceName(wp.trim());
                         // keep minimal fields necessary for department creation
                         uniqueMap.put(newFull, copy);
                     }
                 }
             } catch (Exception ignore) {
                 // ignore malformed rows
             }
         }

         List<HRData> uniqueDeptList = uniqueMap.values().stream()
                 // sort by hierarchy depth: fewer '-' means higher level (parents first), then by name
                 .sorted(Comparator
                         .comparingInt((HRData h) -> countDashes(h.getDepName()))
                         .thenComparing(h -> Optional.ofNullable(h.getDepName()).orElse("")))
                 .collect(Collectors.toList());

         logger.info("需要處理的部門數量 (依完整 DEP_NAME): {}", uniqueDeptList.size());

         for (HRData sampleData : uniqueDeptList) {
             try {
                // skip if department is considered obsolete according to CSV data
                if (isDepartmentObsolete(sampleData.getDepName(), hrDataList)) {
                    logger.info("部門已標記為廢止，跳過: {}", sampleData.getDepName());
                    continue;
                }
                 processSingleDepartment(sampleData, sourceFileName);
             } catch (Exception e) {
                 logger.error("處理部門失敗: {} - {}", sampleData.getDepCode(), sampleData.getDepName(), e);
                 // persist error log
                 insertErrorLog("DEPARTMENT", sampleData.getDepCode(), sampleData, e, sourceFileName);
             }
         }
     }

    /**
     * 判斷該部門是否已廢止 (根據 CSV 原始資料)
     * 邏輯對應原始 Python:
     * 1) 若 CSV 中完全沒有該部門的任何記錄 => 視為廢止 (不存在)
     * 2) 若該部門沒有任何在職人員 (STATE_NAME == '在職')，且其 DEP_NO 或 DEP_CODE 已被其他部門沿用且該部門有在職人員 => 視為廢止
     */
    private boolean isDepartmentObsolete(String deptName, List<HRData> csvData) {
        if (deptName == null || deptName.trim().isEmpty()) return true;
        String target = deptName.trim();

        // 找出該部門的所有記錄
        List<HRData> deptRecords = csvData.stream()
                .filter(r -> r.getDepName() != null && r.getDepName().trim().equals(target))
                .collect(Collectors.toList());

        if (deptRecords.isEmpty()) {
            // CSV 中沒有任何該部門的記錄 -> 視為不存在/廢止
            return true;
        }

        // 檢查是否有在職人員
        boolean hasActive = deptRecords.stream()
                .anyMatch(r -> r.getStateName() != null && r.getStateName().trim().equals("在職"));
        if (hasActive) return false; // 有在職人員 -> 仍在運作

        // 無在職人員，檢查是否有其他部門使用相同的 DEP_NO 或 DEP_CODE 且有在職人員
        String depNo = deptRecords.get(0).getDepNo();
        String depCode = deptRecords.get(0).getDepCode();

        boolean otherUsesSame = csvData.stream()
                .filter(r -> r.getDepName() != null && !r.getDepName().trim().equals(target))
                .filter(r -> r.getStateName() != null && r.getStateName().trim().equals("在職"))
                .anyMatch(r -> (depNo != null && depNo.equals(r.getDepNo())) || (depCode != null && depCode.equals(r.getDepCode())));

        if (otherUsesSame) {
            // 編號被其他部門沿用且該部門有在職人員 -> 原部門應視為廢止
            return true;
        }

        return false; // 預設為未廢止
    }

    private void processSingleDepartment(HRData hrData, String sourceFileName) {
         String fullDeptName = hrData.getDepName();
         String depCode = hrData.getDepCode();
         String depNo = hrData.getDepNo();
         String cpnyId = hrData.getCpnyId();

        String wp = hrData.getWorkplaceName();
        if (fullDeptName != null
                && fullDeptName.contains("商品處-理賠部-理賠一科-")
                && wp != null
                && !wp.trim().isEmpty()
                && !"總公司".equals(wp.trim())) {

            String specificFull = fullDeptName.trim() + "-" + wp.trim();
            fullDeptName = specificFull;

        }
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

        // If dep_code exists in CSV row, prefer updating existing department by dep_code
        UUID existingCusIdByDepCode = null;
        if (depCode != null && !depCode.trim().isEmpty()) {
            try {
                String findByDepCodeSql = "SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"dep_code\" = ? LIMIT 1";
                try {
                    existingCusIdByDepCode = jdbcTemplate.queryForObject(findByDepCodeSql, UUID.class, depCode);
                } catch (EmptyResultDataAccessException ex) {
                    existingCusIdByDepCode = null;
                }
            } catch (Exception e) {
                logger.warn("查詢 dep_code={} 的部門時發生錯誤，將繼續: {}", depCode, e.getMessage());
            }
        }

        if (existingCusIdByDepCode != null) {
            // Update the existing record that matches dep_code
            try {
                String updateSql = "UPDATE public.\"CUS_HRImport_Department\" SET \"cpynid\" = ?, \"dep_no\" = ?, \"name\" = ?, \"full_name\" = ?, \"code\" = ?, \"manager\" = ?, \"parent_code\" = ?, \"description\" = ?, \"tree_level\" = ? WHERE \"dep_code\" = ?";
                jdbcTemplate.update(updateSql,
                        cpnyId,
                        depNo,
                        shortName,
                        fullDeptName,
                        code,
                        "系統管理員",
                        parentDeptCode,
                        depNo,
                        treeLevel,
                        depCode);

                logger.info("發現相同 dep_code，已更新 CUS_HRImport_Department (dep_code={} -> full_name={}, name={}, parent={})", depCode, fullDeptName, shortName, parentDeptCode);
            } catch (Exception e) {
                logger.warn("更新依 dep_code 的部門失敗 dep_code={} err={}", depCode, e.getMessage());
                insertErrorLog("DEPARTMENT", depCode, hrData, e, sourceFileName);
            }
        } else {
            // Fallback: check by full code (original behavior)
            String checkSql = "SELECT COUNT(*) FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?";
            Integer count = 0;
            try {
                count = jdbcTemplate.queryForObject(checkSql, Integer.class, code);
            } catch (EmptyResultDataAccessException ex) {
                count = 0;
            }

            if (count == null || count == 0) {
                // Insert new department
                String insertSql = "INSERT INTO public.\"CUS_HRImport_Department\" (\"id\", \"cpynid\", \"dep_no\", \"dep_code\", \"name\", \"full_name\", \"code\", \"manager\", \"parent_code\", \"description\", \"tree_level\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try {
                    jdbcTemplate.update(insertSql,
                            UUID.randomUUID(), // id
                            cpnyId, depNo, depCode, shortName, fullDeptName, code,
                            "系統管理員", parentDeptCode, depNo, treeLevel);
                    logger.info("新增部門: {} (代碼: {}, 層級: {})", fullDeptName, code, treeLevel);
                } catch (Exception e) {
                    logger.warn("新增部門失敗(code={}): {}", code, e.getMessage());
                    insertErrorLog("DEPARTMENT", depCode, hrData, e, sourceFileName);
                }
            } else {
                // Update existing by code
                String updateSql = "UPDATE public.\"CUS_HRImport_Department\" SET \"cpynid\" = ?, \"dep_no\" = ?, \"dep_code\" = ?, \"name\" = ?, \"full_name\" = ?, \"manager\" = ?, \"parent_code\" = ?, \"description\" = ?, \"tree_level\" = ? WHERE \"code\" = ?";
                try {
                    jdbcTemplate.update(updateSql,
                            cpnyId, depNo, depCode, shortName, fullDeptName, "系統管理員",
                            parentDeptCode, depNo, treeLevel, code);
                    logger.debug("更新部門: {} (代碼: {}, 層級: {})", fullDeptName, code, treeLevel);
                } catch (Exception e) {
                    logger.warn("更新部門失敗(code={}): {}", code, e.getMessage());
                    insertErrorLog("DEPARTMENT", depCode, hrData, e, sourceFileName);
                }
            }
        }

        // Retrieve CUS id: prefer code match, fallback to dep_code if updated by dep_code
        UUID cusId = null;
        try {
            String idSql = "SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?";
            try {
                cusId = jdbcTemplate.queryForObject(idSql, UUID.class, code);
            } catch (EmptyResultDataAccessException ex) {
                if (depCode != null && !depCode.trim().isEmpty()) {
                    try {
                        cusId = jdbcTemplate.queryForObject("SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"dep_code\" = ? LIMIT 1", UUID.class, depCode);
                    } catch (EmptyResultDataAccessException ex2) {
                        cusId = null;
                    }
                } else {
                    cusId = null;
                }
            }
        } catch (Exception e) {
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

        // If a TsDepartment already exists with the same full name, reuse its FId to update that row
        // This ensures we preserve the existing FTreeSerial for that department.
        try {
            if (dept.getFullName() != null && !dept.getFullName().trim().isEmpty()) {
                String existingFId = jdbcTemplate.queryForObject(
                        "SELECT \"FId\" FROM public.\"TsDepartment\" WHERE \"FFullName\" = ? LIMIT 1",
                        String.class,
                        dept.getFullName());
                if (existingFId != null && !existingFId.trim().isEmpty()) {
                    try {
                        UUID fid = UUID.fromString(existingFId);
                        dept.setId(fid);
                        logger.debug("發現已存在的 TsDepartment，將使用其 FId 更新 (FId={} , FFullName={})", existingFId, dept.getFullName());
                    } catch (Exception e) {
                        logger.debug("解析已存在 TsDepartment.FId 失敗: {}", e.getMessage());
                    }
                }
            }
        } catch (EmptyResultDataAccessException ignore) {
            // not found, will insert new
        } catch (Exception e) {
            logger.debug("查詢是否已有 TsDepartment 時發生錯誤，將繼續: {}", e.getMessage());
        }

        // Step 1: 查找 FParentId (父部門的 FId)
        UUID parentFId = null;
        String parentCode = dept.getParentCode();
        if (dept.getTreeLevel() != null && dept.getTreeLevel() == 2) {
            // tree level 2 => top-level department, use configured root GUID from application.yml
            try {
                    parentFId = UUID.fromString(tsRootFid.trim());
            } catch (Exception e) {
                logger.warn("無法解析 app.ts.root-fid='{}'. 使用預設 root FId。錯誤: {}", tsRootFid, e.getMessage());
                insertErrorLog("DEPARTMENT", dept.getCode(), null, e, dept.getFullName());
            }
        } else if (parentCode != null && !parentCode.isEmpty()) {
            try {
                String parentIdSql = "SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?";
                parentFId = jdbcTemplate.queryForObject(parentIdSql, UUID.class, parentCode);
            } catch (EmptyResultDataAccessException e) {
                logger.warn("未找到父部門 (code={}) 的 CUS id，FParentId 設為 NULL。", parentCode);
                parentFId = null;
            } catch (Exception e) {
                logger.error("查找父部門FId時發生錯誤，Code: {}", parentCode, e);
            }
        }

        // compute FTreeSerial based on configured start and parent chain
        // Pass parentFId so computeFTreeSerial can detect parent changes and reassign serial when moved
        String fTreeSerial = computeFTreeSerial(dept, parentFId);

        // Step 2: 使用 PostgreSQL 的 upsert (INSERT ... ON CONFLICT) 插入或更新 TsDepartment
        // TsDepartment 欄位: FId, FParentId, FIndex, FTreeLevel, FTreeSerial, FName, FFullName, FShortCode, FDescription

        String upsertSql =
                "INSERT INTO public.\"TsDepartment\" (\"FId\",\"FParentId\",\"FIndex\",\"FTreeLevel\",\"FTreeSerial\",\"FName\",\"FFullName\",\"FShortCode\",\"FDescription\",\"FUserId\",\"FEnabled\",\"FIsCompany\",\"FIsServices\",\"FIsSales\") " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (\"FId\") DO UPDATE SET " +
                        "\"FParentId\" = EXCLUDED.\"FParentId\", " +
                        "\"FTreeLevel\" = EXCLUDED.\"FTreeLevel\", " +
                        "\"FName\" = EXCLUDED.\"FName\", " +
                        "\"FFullName\" = EXCLUDED.\"FFullName\", " +
                        "\"FShortCode\" = EXCLUDED.\"FShortCode\", " +
                        "\"FDescription\" = EXCLUDED.\"FDescription\", " +
                        "\"FTreeSerial\" = EXCLUDED.\"FTreeSerial\", " +
                        "\"FUserId\" = EXCLUDED.\"FUserId\", " +
                        "\"FIsServices\" = 0, " +
                        "\"FIsSales\" = 0, " +
                        "\"FEnabled\" = 1, " +
                        "\"FIsCompany\" = 0";

        UUID fixedUserId = null;
        try {
            if (tsDeptUserId != null && !tsDeptUserId.trim().isEmpty()) {
                fixedUserId = UUID.fromString(tsDeptUserId.trim());
            }
        } catch (Exception e) {
            logger.warn("無法解析 app.ts.dept-user-id='{}'. 使用預設值。錯誤: {}", tsDeptUserId, e.getMessage());
            insertErrorLog("DEPARTMENT", dept.getCode(), null, e, dept.getFullName());
        }

        // 執行 upsert 語句
        jdbcTemplate.update(upsertSql,
                (dept.getId() == null ? null : dept.getId().toString()),                 // FId
                (parentFId == null ? null : parentFId.toString()),                    // FParentId
                 0,                             // FIndex (預設 0)
                 dept.getTreeLevel() != null ? dept.getTreeLevel() : 1, // FTreeLevel
                 fTreeSerial,                  // FTreeSerial (computed)
                 dept.getName(),               // FName
                 dept.getFullName(),           // FFullName
                 dept.getDep_code(),           // FShortCode (mapped from dep_code)
                 dept.getDescription(),        // FDescription
                (fixedUserId == null ? null : fixedUserId.toString()),                  // FUserId (for insert/update via EXCLUDED)
                 1,                            // FEnabled
                 0,                            // FIsCompany
                 0,                            // FIsServices
                 0                             // FIsSales
         );

        logger.info("TsDepartment 同步: {} ({}) 完成, FParentId: {}, FTreeSerial: {}", dept.getName(), dept.getCode(), parentFId, fTreeSerial);
    }

// 修正 computeFTreeSerial 方法，確保父子部門序號一致性

    /**
     * Compute FTreeSerial for a department based on configured starting serial and parent chain.
     * Rules:
     * - Root group exists as 001 (treeLevel=1) in DB.
     * - Level-2 serials extend root: 001.001, 001.002, ... starting from configured start (startFTreeSerial)
     * - Level-3: 001.001.001, 001.001.002, ... and so on.
     *
     * IMPORTANT: For level-2 departments, check if child departments already exist.
     * If children exist, derive the parent's serial from the child's serial to maintain consistency.
     */
    private String computeFTreeSerial(Department dept, UUID parentFId) {
        String code = dept.getCode();
        Integer level = dept.getTreeLevel() == null ? 1 : dept.getTreeLevel();

        // if level == 1, return the base "001"
        if (level == 1) return "001";

        // If this department already exists in TsDepartment (by FId), preserve its current FTreeSerial.
        // This avoids changing an existing department's serial unless the department's parent has changed.
        // If the stored FParentId differs from the incoming parentFId, we will NOT reuse the old FTreeSerial
        // and will allocate a new one so the department can be placed under the new parent tree.
        try {
            if (dept.getId() != null) {
                try {
                    String q = "SELECT \"FTreeSerial\", \"FParentId\" FROM public.\"TsDepartment\" WHERE \"FId\" = ?";
                    Map<String, Object> row = jdbcTemplate.queryForMap(q, dept.getId().toString());
                    Object fSerialObj = row.get("FTreeSerial");
                    Object fParentObj = row.get("FParentId");
                    String existing = fSerialObj == null ? null : fSerialObj.toString();
                    String existingParent = fParentObj == null ? null : fParentObj.toString();
                    String desiredParent = parentFId == null ? null : parentFId.toString();
                    if (existing != null && !existing.trim().isEmpty() && Objects.equals(existingParent, desiredParent)) {
                        // parent is unchanged -> preserve existing serial
                        return existing;
                    } else if (existing != null && !existing.trim().isEmpty() && !Objects.equals(existingParent, desiredParent)) {
                        // parent changed -> do NOT reuse existing serial; allocate a new one below
                        logger.info("TsDepartment parent changed for {}: oldParent={} newParent={} -> will allocate new FTreeSerial", dept.getFullName(), existingParent, desiredParent);
                    }
                } catch (EmptyResultDataAccessException ex) {
                    // not found, continue to compute a new serial
                }
             }
         } catch (EmptyResultDataAccessException ignore) {
             // no existing TsDepartment for this FId, continue to compute a new serial
         } catch (Exception e) {
             logger.debug("查詢現有 TsDepartment.FTreeSerial 時發生例外，將繼續分配新序號: {}", e.getMessage());
         }

        // get configured start suffix (e.g., startFTreeSerial = "001.001")
        String start = (startFTreeSerial == null) ? "001.001" : startFTreeSerial;
        int startSuffixNum = 1;
        if (start.contains(".")) {
            String sfx = start.substring(start.indexOf('.') + 1);
            try { startSuffixNum = Integer.parseInt(sfx); } catch (Exception e) { startSuffixNum = 1; }
        }

        try {
            if (level == 2) {
                // PRIORITY 1: Check if this department already has children in TsDepartment
                // If so, derive parent serial from existing children to maintain consistency
                String childCheckSql = "SELECT \"FTreeSerial\" FROM public.\"TsDepartment\" " +
                        "WHERE \"FFullName\" LIKE ? AND \"FTreeLevel\" > 2 " +
                        "ORDER BY \"FTreeSerial\" LIMIT 1";
                try {
                    // Search for children whose FFullName starts with this department's name
                    // e.g., if dept.getName() is "總經理", find "總經理直轄-法令遵循部-法令遵循二科"
                    String childPattern = dept.getName() + "%";
                    List<String> childSerials = jdbcTemplate.queryForList(childCheckSql, String.class, childPattern);

                    if (childSerials != null && !childSerials.isEmpty()) {
                        String childSerial = childSerials.get(0);
                        // Extract parent serial from child (e.g., "001.001.001" -> "001.001")
                        String[] segments = childSerial.split("\\.");
                        if (segments.length >= 2) {
                            String derivedSerial = segments[0] + "." + segments[1];
                            logger.info("從現有子部門推導父部門序號: {} -> {} (基於子部門序號: {})",
                                    dept.getName(), derivedSerial, childSerial);
                            return derivedSerial;
                        }
                    }
                } catch (Exception e) {
                    logger.debug("檢查子部門以推導父序號時發生錯誤: {}", e.getMessage());
                }

                // collect all existing serials that start with '001.' (include deeper nodes)
                String likePattern = "001.%";
                String sql = "SELECT \"FTreeSerial\" FROM public.\"TsDepartment\" WHERE \"FTreeSerial\" LIKE ?";
                List<String> all = jdbcTemplate.queryForList(sql, String.class, likePattern);

                // build set of used level-2 suffix numbers (from both level-2 rows and deeper rows)
                Set<Integer> used = new HashSet<>();
                if (all != null) {
                    for (String f : all) {
                        if (f == null) continue;
                        String[] segs = f.split("\\.");
                        if (segs.length >= 2) {
                            try { used.add(Integer.parseInt(segs[1])); } catch (Exception ignore) {}
                        }
                    }
                }

                // PRIORITY 2: find deeper TsDepartment rows whose FFullName tokens indicate they belong under this department
                Set<Integer> candidateFromName = new HashSet<>();
                try {
                    String sqlAll = "SELECT \"FTreeSerial\", \"FFullName\" FROM public.\"TsDepartment\" WHERE \"FTreeSerial\" LIKE ?";
                    List<Map<String, Object>> rows = jdbcTemplate.queryForList(sqlAll, likePattern);
                    if (rows != null) {
                        for (Map<String, Object> row : rows) {
                            Object fObj = row.get("FTreeSerial");
                            Object fullObj = row.get("FFullName");
                            if (fObj == null || fullObj == null) continue;
                            String fSerial = fObj.toString();
                            String fFull = fullObj.toString();
                            // tokenize full name by '-' and check if any token equals dept.getName
                            String[] tokens = fFull.split("\\-");
                            boolean matches = false;
                            for (String tok : tokens) {
                                if (tok == null) continue;
                                if (tok.trim().equals(dept.getName())) { matches = true; break; }
                                // also accept tokens that start with dept name (e.g., '總經理直轄' startsWith '總經理')
                                if (tok.trim().startsWith(dept.getName())) { matches = true; break; }
                            }
                            if (matches) {
                                String[] segs = fSerial.split("\\.");
                                if (segs.length >= 2) {
                                    try { candidateFromName.add(Integer.parseInt(segs[1])); } catch (Exception ignore) {}
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.debug("搜尋 TsDepartment 以推斷 level-2 前綴失敗: {}", e.getMessage());
                }

                // If we found candidate suffixes from existing names, prefer the smallest one not already used as a level-2 row
                if (!candidateFromName.isEmpty()) {
                    List<Integer> sorted = new ArrayList<>(candidateFromName);
                    Collections.sort(sorted);
                    for (Integer cand : sorted) {
                        try {
                            String serial = String.format("001.%03d", cand);
                            Integer cntLevel2 = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM public.\"TsDepartment\" WHERE \"FTreeSerial\" = ? AND \"FTreeLevel\" = 2", Integer.class, serial);
                            if (cntLevel2 == null || cntLevel2 == 0) {
                                logger.info("從名稱推導父部門序號: {} -> {}", dept.getName(), serial);
                                return serial;
                            }
                        } catch (Exception ex) {
                            // ignore and continue
                        }
                    }
                    // If all candidates already have level-2 rows, fall through to normal allocation
                }

                // PRIORITY 3: find the first available suffix >= startSuffixNum
                int candidate = startSuffixNum;
                while (candidate < 1000000) {
                    if (!used.contains(candidate)) {
                        String serial = String.format("001.%03d", candidate);
                        logger.info("分配新的父部門序號: {} -> {}", dept.getName(), serial);
                        return serial;
                    }
                    candidate++;
                }

                // fallback: return start
                return String.format("001.%03d", startSuffixNum);
            } else {
                // level >=3: build parent FTreeSerial and then append next suffix
                String parentCodeFull = dept.getParentCode();
                if (parentCodeFull == null) {
                    // fallback
                    return "001";
                }
                UUID parentCusId = jdbcTemplate.queryForObject("SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?", UUID.class, parentCodeFull);
                String parentFTree = jdbcTemplate.queryForObject("SELECT \"FTreeSerial\" FROM public.\"TsDepartment\" WHERE \"FId\" = ?", String.class, parentCusId == null ? null : parentCusId.toString());
                if (parentFTree == null) parentFTree = "001";

                // lookup existing children under parentFTree (children will have prefix parentFTree+'.%')
                String likePattern = parentFTree + ".%";
                String sql = "SELECT \"FTreeSerial\" FROM public.\"TsDepartment\" WHERE \"FTreeSerial\" LIKE ?";
                List<String> existing = jdbcTemplate.queryForList(sql, String.class, likePattern);

                // build set of numeric last-segments among existing children that are direct children of parent
                Set<Integer> used = new HashSet<>();
                if (existing != null) {
                    for (String f : existing) {
                        if (f == null) continue;
                        String[] segs = f.split("\\.");
                        if (segs.length == (parentFTree.split("\\.").length + 1)) {
                            try { used.add(Integer.parseInt(segs[segs.length - 1])); } catch (Exception ignore) {}
                        }
                    }
                }

                int nextIndex = 1;
                // find smallest unused >0
                while (nextIndex < 1000000) {
                    if (!used.contains(nextIndex)) break;
                    nextIndex++;
                }

                return String.format(parentFTree + ".%03d", nextIndex);
            }
        } catch (Exception e) {
            logger.warn("計算 FTreeSerial 發生錯誤，fallback 為 001: {}", e.getMessage());
            return "001";
        }
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
        String depCode = hrData.getDepCode();
        String depName = hrData.getDepName();
        String depNo = hrData.getDepNo();
        String depFullName = hrData.getDepName(); // init depName. Change it below.

        if (workcard == null || workcard.trim().isEmpty()) {
            logger.warn("員工編號為空，跳過處理。員工姓名: {}", empName);
            return;
        }

        logger.debug("處理員工: {} ({})", empName, workcard);

        try {
            // 檢查帳號是否存在
            String checkAccountSql = "SELECT COUNT(*) FROM public.\"TsAccount\" WHERE \"FLoginName\" = ?";
            Integer accountCount = jdbcTemplate.queryForObject(checkAccountSql, Integer.class, workcard);

            UUID departmentId = null;

            /* =========================
             * 特殊部門處理（依 workplace）
             * ========================= */
            try {
                String wp = hrData.getWorkplaceName();
                if (depName != null
                        && depName.contains("商品處-理賠部-理賠一科-")
                        && wp != null
                        && !wp.trim().isEmpty()
                        && !"總公司".equals(wp.trim())) {

                    String specificFull = depName.trim() + "-" + wp.trim();
                    depFullName = specificFull; // update dep full_name for employee assigned working location.

                    String[] segs = specificFull.split("-");
                    String parentOfSpecific =
                            segs.length > 1 ? String.join("-", Arrays.copyOf(segs, segs.length - 1)) : null;

                    if (parentOfSpecific != null) {
                        ensureParentChain(parentOfSpecific, sourceFileName);
                    }

                    Integer cntExist = jdbcTemplate.queryForObject(
                            "SELECT COUNT(*) FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?",
                            Integer.class,
                            specificFull
                    );

                    if (cntExist == null || cntExist == 0) {
                        String shortName = segs[segs.length - 1].trim();
                        int treeLevelForSpecific = segs.length + 1;

                        String insertSql =
                                "INSERT INTO public.\"CUS_HRImport_Department\" " +
                                        "(\"id\",\"cpynid\",\"dep_no\",\"dep_code\",\"name\",\"full_name\",\"code\",\"manager\",\"parent_code\",\"description\",\"tree_level\") " +
                                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

                        jdbcTemplate.update(
                                insertSql,
                                UUID.randomUUID(),
                                hrData.getCpnyId(),
                                depNo,
                                depCode,
                                shortName,
                                specificFull,
                                specificFull,
                                "系統管理員",
                                parentOfSpecific,
                                depNo,
                                treeLevelForSpecific
                        );

                        // 同步 TsDepartment
                        UUID cusId = jdbcTemplate.queryForObject(
                                "SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?",
                                UUID.class,
                                specificFull
                        );

                        if (cusId != null) {
                            Department dept = new Department();
                            dept.setId(cusId);
                            dept.setCpynid(hrData.getCpnyId());
                            dept.setDep_no(depNo);
                            dept.setDep_code(depCode);
                            dept.setName(shortName);
                            dept.setFullName(specificFull);
                            dept.setCode(specificFull);
                            dept.setManager("系統管理員");
                            dept.setParentCode(parentOfSpecific);
                            dept.setDescription(depNo);
                            dept.setTreeLevel(treeLevelForSpecific);
                            insertOrUpdateTsDepartment(dept);
                        }
                    }

                    departmentId = jdbcTemplate.queryForObject(
                            "SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?",
                            UUID.class,
                            specificFull
                    );
                }
            } catch (Exception e) {
                logger.warn("特定部門處理失敗，回退 dep_code 查詢: {}", e.getMessage());
            }

            /* =========================
             * fallback：依 depFullName 查部門
             * ========================= */
            if (departmentId == null) {
                String deptIdSql =
                        "SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"full_name\" = ?";
                List<UUID> departmentIds =
                        jdbcTemplate.queryForList(deptIdSql, UUID.class, depFullName);

                if (departmentIds != null && !departmentIds.isEmpty()) {
                    departmentId = departmentIds.get(0);
                    if (departmentIds.size() > 1) {
                        logger.warn("depFullName = {}. dep_code={} 對應多筆部門，使用第一筆 id={}", depFullName, depCode, departmentId);
                        insertErrorLog(
                                "EMPLOYEE",
                                workcard,
                                hrData,
                                new RuntimeException("Multiple departments for depFullName=" + depFullName),
                                sourceFileName
                        );
                    }
                }
            }

            if (departmentId == null) {
                throw new RuntimeException("Department not found for depFullName=" + depFullName);
            }

            /* =========================
             * 查詢部門層級（用 id）
             * ========================= */
            Integer treeLevel = null;
            try {
                treeLevel = jdbcTemplate.queryForObject(
                        "SELECT \"tree_level\" FROM public.\"CUS_HRImport_Department\" WHERE \"id\" = ?",
                        Integer.class,
                        departmentId
                );
            } catch (Exception e) {
                logger.warn("查詢部門層級失敗 depId={}", departmentId);
            }

            /* =========================
             * 建立或更新員工
             * ========================= */
            if (accountCount == null || accountCount == 0) {
                createNewEmployee(hrData, departmentId);
            } else {
                updateEmployee(hrData, departmentId, treeLevel);
            }

        } catch (Exception e) {
            logger.error("處理員工資料失敗: {} ({})", empName, workcard, e);
            insertErrorLog("EMPLOYEE", workcard, hrData, e, sourceFileName);
            throw new RuntimeException("員工處理失敗", e);
        }
    }

    private void createNewEmployee(HRData hrData, UUID departmentId) {
        UUID employeeId = UUID.randomUUID();
        UUID identityId = UUID.randomUUID();

        // Determine whether the created account/user should be enabled.
        boolean shouldBeEnabled = (enabledStates != null && hrData.getStateNo() != null && enabledStates.contains(hrData.getStateNo()));
        int enabledFlag = shouldBeEnabled ? 1 : 0;

        // 1) 插入 TsUser（先建立員工資料）
        String userSql = "INSERT INTO public.\"TsUser\" (\"FId\", \"FName\", \"FLoginName\", \"FPassword\", \"FDepartmentId\", " +
                "\"FMobile\", \"FEmail\", \"FEnabled\", \"U_EmployeeCore\", \"FOnGuard\", \"FLanguage\") " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'zh-TW')";

        jdbcTemplate.update(userSql, employeeId.toString(), hrData.getEmpName(), hrData.getWorkcard(),
                defaultPassword, (departmentId == null ? null : departmentId.toString()), hrData.getMobile(), "", enabledFlag,
                hrData.getWorkcard());

        // After TsUser inserted, confirm TsAccount existence
        String findAccountSql = "SELECT \"FId\" FROM public.\"TsAccount\" WHERE \"FLoginName\" = ?";
        String accountIdStr = null;
        UUID accountId = null;
        try {
            accountIdStr = jdbcTemplate.queryForObject(findAccountSql, String.class, hrData.getWorkcard());
        } catch (EmptyResultDataAccessException ex) {
            // not found -> will create below
        } catch (Exception ex) {
            logger.warn("查詢 TsAccount.FId 發生錯誤: {}", ex.getMessage());
        }

        try {
            if (accountIdStr == null || accountIdStr.trim().isEmpty()) {
                // TsAccount 不存在 -> 建立 TsAccount
                accountId = UUID.randomUUID();
                String accountSql = "INSERT INTO public.\"TsAccount\" (\"FId\", \"FName\", \"FLoginName\", \"FPassword\", \"FEmail\", \"FMobile\", \"FCreateTime\", \"FEnabled\", \"FLanguage\") VALUES (?, ?, ?, ?, ?, ?, now(), ?, ?)";
                jdbcTemplate.update(accountSql, accountId.toString(), hrData.getEmpName(), hrData.getWorkcard(),
                        defaultPassword, "", hrData.getMobile(), enabledFlag, "zh-TW");
                accountIdStr = accountId.toString();
                logger.info("TsAccount 不存在，已建立: {} (login={})", accountIdStr, hrData.getWorkcard());

                // 建立 TsAccountIdentity（因為 account 剛建立，identity 一定不存在）
                try {
                    String identitySql = "INSERT INTO public.\"TsAccountIdentity\" (\"FId\", \"FName\", \"FAccountId\", \"FIdentityTypeId\", \"FEntityId\", \"FDefault\", \"FIndex\") VALUES (?, ?, ?, ?, ?, 1, 0)";
                    jdbcTemplate.update(identitySql, identityId.toString(), hrData.getEmpName(), accountIdStr, defaultIdentityTypeId, employeeId.toString());
                    logger.info("已建立 TsAccountIdentity: account={} employee={}", accountIdStr, employeeId);
                } catch (Exception ie) {
                    logger.warn("建立 TsAccountIdentity 失敗 (account={} employee={}): {}", accountIdStr, employeeId, ie.getMessage());
                    try { insertErrorLog("EMPLOYEE", hrData.getWorkcard(), hrData, ie, null); } catch (Exception ignore) {}
                }
            } else {
                // TsAccount 已存在 -> 確認 TsAccountIdentity 是否存在
                logger.info("發現已存在 TsAccount: {} (login={})", accountIdStr, hrData.getWorkcard());
                try {
                    accountId = UUID.fromString(accountIdStr.trim());
                } catch (Exception e) {
                    // keep accountId null if parsing fails
                }

                try {
                    String checkIdentitySql = "SELECT COUNT(*) FROM public.\"TsAccountIdentity\" WHERE \"FAccountId\" = ? AND \"FEntityId\" = ?";
                    Integer cnt = jdbcTemplate.queryForObject(checkIdentitySql, Integer.class, accountIdStr, employeeId.toString());
                    if (cnt == null || cnt == 0) {
                        // identity 不存在 -> 建立 TsAccountIdentity
                        String identitySql = "INSERT INTO public.\"TsAccountIdentity\" (\"FId\", \"FName\", \"FAccountId\", \"FIdentityTypeId\", \"FEntityId\", \"FDefault\", \"FIndex\") VALUES (?, ?, ?, ?, ?, 1, 0)";
                        jdbcTemplate.update(identitySql, identityId.toString(), hrData.getEmpName(), accountIdStr, defaultIdentityTypeId, employeeId.toString());
                        logger.info("TsAccountIdentity 不存在，已建立: account={} employee={}", accountIdStr, employeeId);
                    } else {
                        logger.debug("TsAccountIdentity 已存在: account={} employee={}", accountIdStr, employeeId);
                    }
                } catch (Exception ex) {
                    logger.warn("檢查或建立 TsAccountIdentity 發生錯誤: {}", ex.getMessage());
                    try { insertErrorLog("EMPLOYEE", hrData.getWorkcard(), hrData, ex, null); } catch (Exception ignore) {}
                }
            }
        } catch (Exception ex) {
            logger.warn("處理 TsAccount/TsAccountIdentity 時發生未預期錯誤: {}", ex.getMessage());
            try { insertErrorLog("EMPLOYEE", hrData.getWorkcard(), hrData, ex, null); } catch (Exception ignore) {}
        }

        // 最後：寫入 TsRoleUser（使用 TsAccount.FId 作為 FUserId）若有設定
        if (tsRoleUserRoleId != null && !tsRoleUserRoleId.trim().isEmpty()) {
            try {
                if (accountIdStr != null && !accountIdStr.trim().isEmpty()) {
                    String insertRoleUserSql = "INSERT INTO public.\"TsRoleUser\" (\"FRoleId\",\"FUserId\") VALUES (?,?) ON CONFLICT (\"FRoleId\",\"FUserId\") DO NOTHING";
                    jdbcTemplate.update(insertRoleUserSql, tsRoleUserRoleId, accountIdStr);
                    logger.info("已將角色關聯寫入 TsRoleUser: role={} userId={}", tsRoleUserRoleId, accountIdStr);
                } else {
                    logger.warn("無法取得 TsAccount.FId，跳過寫入 TsRoleUser (login={})", hrData.getWorkcard());
                }
            } catch (Exception e) {
                logger.warn("寫入 TsRoleUser 失敗: role={} userId={} err={} ", tsRoleUserRoleId, accountIdStr, e.getMessage());
                try { insertErrorLog("EMPLOYEE", hrData.getWorkcard(), hrData, e, null); } catch (Exception ignore) {}
            }
        } else {
            logger.debug("未設定 app.identity-type-id.rol_user_id，跳過寫入 TsRoleUser。");
        }

        logger.info("新增員工並同步帳號/身分/角色完成: {} ({})", hrData.getEmpName(), hrData.getWorkcard());
    }
    
    private void updateEmployee(HRData hrData, UUID departmentId, Integer treeLevel) {
        // 只有部門層級 < treeLabelUpdateDep 時才更新部門
        String userUpdateSql;
        int threshold = (treeLabelUpdateDep == null) ? 4 : treeLabelUpdateDep.intValue();
        if (treeLevel != null && treeLevel < threshold) {
            userUpdateSql = "UPDATE public.\"TsUser\" SET \"FName\" = ?, \"FMobile\" = ?, \"FDepartmentId\" = ?, \"U_EmployeeCore\" = ? " +
                    "WHERE \"FLoginName\" = ?";
            // set U_EmployeeCore to the current login name (workcard)
            jdbcTemplate.update(userUpdateSql,
                    hrData.getEmpName(),
                    hrData.getMobile(),
                    (departmentId == null ? null : departmentId.toString()),
                    hrData.getWorkcard(), // U_EmployeeCore
                    hrData.getWorkcard()); // WHERE FLoginName = ?
         } else {
             userUpdateSql = "UPDATE public.\"TsUser\" SET \"FName\" = ?, \"FMobile\" = ?, \"U_EmployeeCore\" = ? WHERE \"FLoginName\" = ?";
             jdbcTemplate.update(userUpdateSql, hrData.getEmpName(), hrData.getMobile(), hrData.getWorkcard(), hrData.getWorkcard());
         }

        // 更新TsAccount
        String accountUpdateSql = "UPDATE public.\"TsAccount\" SET \"FName\" = ?, \"FMobile\" = ? WHERE \"FLoginName\" = ?";
        jdbcTemplate.update(accountUpdateSql, hrData.getEmpName(), hrData.getMobile(),
                hrData.getWorkcard());
        
        // 新增/確保 TsRoleUser 關聯存在：先查出 TsAccount.FId，然後 upsert TsRoleUser
        if (tsRoleUserRoleId != null && !tsRoleUserRoleId.trim().isEmpty()) {
            try {
                // 先查出 TsAccount.FId（以 String 讀出，避免 JDBC 將 UUID 轉成 String 導致類型轉換錯誤）
                String findAccountIdSql = "SELECT \"FId\" FROM public.\"TsAccount\" WHERE \"FLoginName\" = ?";
                String accountIdStr = null;
                try {
                    accountIdStr = jdbcTemplate.queryForObject(findAccountIdSql, String.class, hrData.getWorkcard());
                } catch (EmptyResultDataAccessException ex) {
                    logger.warn("更新 TsRoleUser 時未找到 TsAccount (loginName={})", hrData.getWorkcard());
                } catch (Exception ex) {
                    logger.warn("查詢 TsAccount.FId 失敗: {}", ex.getMessage());
                }

                // 解析為 UUID (若可解析)，並以字串形式作為 FUserId 更新或插入 TsRoleUser
                String fUserIdForSql = null;
                if (accountIdStr != null && !accountIdStr.trim().isEmpty()) {
                    try {
                        // validate UUID string
                        UUID parsed = UUID.fromString(accountIdStr.trim());
                        fUserIdForSql = parsed.toString();
                    } catch (Exception e) {
                        // 如果無法解析為 UUID，仍嘗試使用原始字串（有些 DB driver/欄位可能已經是字串格式）
                        logger.warn("解析 TsAccount.FId 為 UUID 失敗，將使用原始字串: {}", accountIdStr);
                        fUserIdForSql = accountIdStr;
                    }
                }

                // 使用 UPDATE 來設定角色對應 (直接更新，如果沒有任何列被更新則改為 INSERT)
                String updateRoleUserSql = "UPDATE public.\"TsRoleUser\" SET \"FRoleId\" = ? WHERE \"FUserId\" = ?";
                int affected = 0;
                try {
                    affected = jdbcTemplate.update(updateRoleUserSql, tsRoleUserRoleId, fUserIdForSql);
                } catch (Exception e) {
                    logger.warn("更新 TsRoleUser 失敗 (role={} userId={}): {}", tsRoleUserRoleId, fUserIdForSql, e.getMessage());
                }

                if (affected > 0) {
                    logger.info("已更新 TsRoleUser: role={} userId={} affected={}", tsRoleUserRoleId, fUserIdForSql, affected);
                } else {
                    // 沒有更新任何列 — 代表尚無對應的 TsRoleUser 紀錄，改為新增一筆紀錄
                    logger.info("更新 TsRoleUser 沒有影響任何列（尚無 TsRoleUser 紀錄），將嘗試新增一筆紀錄: role={} userId={}", tsRoleUserRoleId, fUserIdForSql);
                    try {
                        String insertRoleUserSql = "INSERT INTO public.\"TsRoleUser\" (\"FRoleId\",\"FUserId\") VALUES (?,?) ON CONFLICT (\"FRoleId\",\"FUserId\") DO NOTHING";
                        jdbcTemplate.update(insertRoleUserSql, tsRoleUserRoleId, fUserIdForSql);
                        logger.info("已新增 TsRoleUser（或已存在）: role={} userId={}", tsRoleUserRoleId, fUserIdForSql);
                    } catch (Exception ie) {
                        logger.warn("新增 TsRoleUser 失敗: role={} userId={} err={}", tsRoleUserRoleId, fUserIdForSql, ie.getMessage());
                        try { insertErrorLog("EMPLOYEE", hrData.getWorkcard(), hrData, ie, null); } catch (Exception ignore) {}
                    }
                }
            } catch (Exception ex) {
                logger.warn("更新 TsRoleUser 時發生錯誤: {}", ex.getMessage());
            }
        }

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

            // build new archive filename: HrImport_YYYYMMDDhhmmss + original extension
            String ext = "";
            int idx = originalName.lastIndexOf('.');
            if (idx >= 0) {
                ext = originalName.substring(idx); // includes the dot
            } else {
                // default to .csv if original has no extension
                ext = ".csv";
            }

            String archivedName = "HrImport_" + timestamp + ext;
            Path target = archiveDir.resolve(archivedName);

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
            String payload = hrData == null ? null : hrData.toString();
            String errMsg = ex == null ? "" : ex.getMessage();
            String stack = null;
            if (ex != null) {
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                stack = sw.toString();
            }

            // Delegate to ErrorLogService which will run in REQUIRES_NEW transaction
            errorLogService.insertErrorLog(recordType, recordKey, payload, errMsg, stack, fileName);
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
                String checkSql = "SELECT COUNT(*) FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?";
                Integer cnt = jdbcTemplate.queryForObject(checkSql, Integer.class, code);
                if (cnt == null || cnt == 0) {
                    String insertSql = "INSERT INTO public.\"CUS_HRImport_Department\" " +
                            "(\"id\", \"cpynid\", \"dep_no\", \"dep_code\", \"name\", \"full_name\", \"code\", \"manager\", \"parent_code\", \"description\", \"tree_level\") " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    jdbcTemplate.update(insertSql,
                            UUID.randomUUID(), // id
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
                        UUID cusId = jdbcTemplate.queryForObject("SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?", UUID.class, code);
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
            Integer cnt = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?", Integer.class, code);
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
            String insertSql = "INSERT INTO public.\"CUS_HRImport_Department\" " +
                    "(\"id\", \"cpynid\", \"dep_no\", \"dep_code\", \"name\", \"full_name\", \"code\", \"manager\", \"parent_code\", \"description\", \"tree_level\") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            jdbcTemplate.update(insertSql,
                    UUID.randomUUID(), // id
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
                UUID cusId = jdbcTemplate.queryForObject("SELECT \"id\" FROM public.\"CUS_HRImport_Department\" WHERE \"code\" = ?", UUID.class, code);
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
