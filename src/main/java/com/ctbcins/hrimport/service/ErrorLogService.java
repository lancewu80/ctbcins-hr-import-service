package com.ctbcins.hrimport.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ErrorLogService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void insertErrorLog(String recordType, String recordKey, String payload, String errorMessage, String stackTrace, String fileName) {
        String insertSql = "INSERT INTO public.\"CUS_HRImport_Error_Log\" (id, file_name, record_type, record_key, payload, error_message, stack_trace, processed_at, created_by) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, now(), ?)";

        UUID id = UUID.randomUUID();
        jdbcTemplate.update(insertSql,
                id,
                fileName,
                recordType,
                recordKey,
                payload,
                errorMessage,
                stackTrace,
                "hr-import-service"
        );
    }
}

