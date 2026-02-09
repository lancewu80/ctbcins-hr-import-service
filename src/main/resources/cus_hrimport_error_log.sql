-- T-SQL DDL: CUS_HRImport_Error_Log (SQL Server)
IF OBJECT_ID('dbo.CUS_HRImport_Error_Log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.CUS_HRImport_Error_Log (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        file_name NVARCHAR(260) NULL,
        record_type NVARCHAR(50) NULL, -- 'DEPARTMENT' or 'EMPLOYEE' or 'RAW'
        record_key NVARCHAR(200) NULL, -- identifier like dep_code or emp_workcard
        payload NVARCHAR(MAX) NULL,    -- full record data (JSON or CSV line)
        error_message NVARCHAR(2000) NULL,
        stack_trace NVARCHAR(MAX) NULL,
        processed_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        created_by NVARCHAR(100) DEFAULT 'hr-import-service'
    );

    -- nonclustered index on record_key for lookup
    CREATE NONCLUSTERED INDEX IDX_CUS_HRImport_Error_Log_RecordKey ON dbo.CUS_HRImport_Error_Log(record_key);
END
GO

-- Sample query to inspect recent errors:
-- SELECT TOP 100 id, file_name, record_type, record_key, error_message, processed_at
-- FROM dbo.CUS_HRImport_Error_Log
-- ORDER BY processed_at DESC;
