-- PostgreSQL translation of schema.sql
-- Preserves original table and column name casing by quoting identifiers.

-- Enable UUID generation (uuid-ossp) for uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CUS_HRImport_Department
CREATE TABLE IF NOT EXISTS "CUS_HRImport_Department" (
    "id" UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "cpynid" VARCHAR(50),
    "dep_no" VARCHAR(50),
    "dep_code" VARCHAR(50),
    "name" VARCHAR(200) NOT NULL,
    "full_name" VARCHAR(200) NOT NULL,
    "code" VARCHAR(50) UNIQUE NOT NULL,
    "manager" VARCHAR(100) DEFAULT '系統管理員',
    "parent_code" VARCHAR(50),
    "description" VARCHAR(500),
    "tree_level" INTEGER,
    "created_date" TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    "updated_date" TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

-- Indexes for CUS_HRImport_Department
CREATE INDEX "IX_CUS_HRImport_Department_Code" ON "CUS_HRImport_Department"("code");
CREATE INDEX "IX_CUS_HRImport_Department_ParentCode" ON "CUS_HRImport_Department"("parent_code");
CREATE INDEX "IX_CUS_HRImport_Department_dep_no" ON "CUS_HRImport_Department"("dep_no");
CREATE INDEX "IX_CUS_HRImport_Department_dep_code" ON "CUS_HRImport_Department"("dep_code");


-- CUS_HRImport
CREATE TABLE IF NOT EXISTS "CUS_HRImport" (
    "id" UUID NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    "cpnyid" VARCHAR(50) NULL,
    "dep_no" VARCHAR(50) NULL,
    "dep_code" VARCHAR(50) NULL,
    "dep_name" VARCHAR(200) NULL,
    "state_no" VARCHAR(10) NULL,
    "state_name" VARCHAR(50) NULL,
    "emp_id" VARCHAR(50) NULL,
    "emp_name" VARCHAR(100) NULL,
    "workcard" VARCHAR(50) NULL,
    "inadate" DATE NULL,
    "quitdate" DATE NULL,
    "stop_w" VARCHAR(50) NULL,
    "start_w" VARCHAR(50) NULL,
    "mdate" DATE NULL,
    "position_name" VARCHAR(100) NULL,
    "mobile" VARCHAR(50) NULL,
    "title_name" VARCHAR(100) NULL,
    "workplace_name" VARCHAR(100) NULL,
    "file_name" VARCHAR(260) NULL,
    "created_at" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX "IX_CUS_HRImport_dep_code" ON "CUS_HRImport"("dep_code");
CREATE INDEX "IX_CUS_HRImport_cpnyid" ON "CUS_HRImport"("cpnyid");
CREATE INDEX "IX_CUS_HRImport_file_name" ON "CUS_HRImport"("file_name");


-- CUS_HRImport_Error_Log
CREATE TABLE IF NOT EXISTS "CUS_HRImport_Error_Log" (
    "id" UUID NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    "file_name" VARCHAR(260) NULL,
    "record_type" VARCHAR(50) NULL,
    "record_key" VARCHAR(200) NULL,
    "payload" TEXT NULL,
    "error_message" VARCHAR(2000) NULL,
    "stack_trace" TEXT NULL,
    "processed_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    "created_by" VARCHAR(100) DEFAULT 'hr-import-service'
);

CREATE INDEX "IDX_CUS_HRImport_Error_Log_RecordKey" ON "CUS_HRImport_Error_Log"("record_key");



