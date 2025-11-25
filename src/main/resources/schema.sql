-- 建立自訂部門表格
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CUS_HRImport_Department' AND xtype='U')
CREATE TABLE CUS_HRImport_Department (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
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