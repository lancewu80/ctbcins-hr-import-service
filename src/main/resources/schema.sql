-- 建立自訂部門表格
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CUS_HRImport_Department' AND xtype='U')
CREATE TABLE CUS_HRImport_Department (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    cpynid NVARCHAR(50),
    dep_no NVARCHAR(50),
    dep_code NVARCHAR(50),
    name NVARCHAR(200) NOT NULL,
    full_name NVARCHAR(200) NOT NULL,
    code NVARCHAR(50) UNIQUE NOT NULL,
    manager NVARCHAR(100) DEFAULT '系統管理員',
    parent_code NVARCHAR(50),
    description NVARCHAR(500),
    tree_level INT,
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME DEFAULT GETDATE()
);

-- 建立索引
CREATE INDEX IX_CUS_HRImport_Department_Code ON CUS_HRImport_Department(code);
CREATE INDEX IX_CUS_HRImport_Department_ParentCode ON CUS_HRImport_Department(parent_code);
CREATE INDEX IX_CUS_HRImport_Department_dep_no ON CUS_HRImport_Department(dep_no);
CREATE INDEX IX_CUS_HRImport_Department_dep_code ON CUS_HRImport_Department(dep_code);


-- sql
Drop table dbo.CUS_HRImport;
CREATE TABLE dbo.CUS_HRImport (
    id uniqueidentifier NOT NULL DEFAULT NEWID() PRIMARY KEY,
    cpnyid nvarchar(50) NULL,
    dep_no nvarchar(50) NULL,
    dep_code nvarchar(50) NULL,
    dep_name nvarchar(200) NULL,
    state_no nvarchar(10) NULL,
    state_name nvarchar(50) NULL,
    emp_id nvarchar(50) NULL,
    emp_name nvarchar(100) NULL,
    workcard nvarchar(50) NULL,
    inadate date NULL,
    quitdate date NULL,
    stop_w nvarchar(50) NULL,
    start_w nvarchar(50) NULL,
    mdate date NULL,
    position_name nvarchar(100) NULL,
    mobile nvarchar(50) NULL,
    title_name nvarchar(100) NULL,
    workplace_name nvarchar(100) NULL,
    file_name nvarchar(260) NULL,
    created_at datetime NOT NULL DEFAULT GETDATE()
);
CREATE INDEX IX_CUS_HRImport_dep_code ON dbo.CUS_HRImport(dep_code);
CREATE INDEX IX_CUS_HRImport_cpnyid ON dbo.CUS_HRImport(cpnyid);
CREATE INDEX IX_CUS_HRImport_file_name ON dbo.CUS_HRImport(file_name);
