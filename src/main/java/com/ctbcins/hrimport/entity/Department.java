package com.ctbcins.hrimport.entity;

import javax.persistence.*;
import java.util.UUID;

@Entity
@Table(name = "CUS_HRImport_Department")
public class Department {
    @Id
    @GeneratedValue
    @Column(columnDefinition = "uniqueidentifier")
    private UUID id;

    @Column(name = "cpynid",columnDefinition = "nvarchar(50)")
    private String cpynid;

    @Column(name = "dep_no",columnDefinition = "nvarchar(50)")
    private String dep_no;

    @Column(name = "dep_code",columnDefinition = "nvarchar(50)")
    private String dep_code;

    @Column(name = "name",columnDefinition = "nvarchar(200)")
    private String name;
    
    @Column(name = "full_name", columnDefinition = "nvarchar(200)")
    private String fullName;
    
    @Column(name = "code", columnDefinition = "nvarchar(50)", unique = true)
    private String code;
    
    @Column(name = "manager", columnDefinition = "nvarchar(100)")
    private String manager = "系統管理員";
    
    @Column(name = "parent_code", columnDefinition = "nvarchar(50)")
    private String parentCode;
    
    @Column(name = "description", columnDefinition = "nvarchar(500)")
    private String description;
    
    @Column(name = "tree_level")
    private Integer treeLevel;
    
    // getters and setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    
    public String getCpynid() { return cpynid; }
    public void setCpynid(String cpynid) { this.cpynid = cpynid; }

    public String getDep_no() { return dep_no; }
    public void setDep_no(String dep_no) { this.dep_no = dep_no; }

    public String getDep_code() { return dep_code; }
    public void setDep_code(String dep_code) { this.dep_code = dep_code; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getFullName() { return fullName; }
    public void setFullName(String fullName) { this.fullName = fullName; }
    
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }
    
    public String getManager() { return manager; }
    public void setManager(String manager) { this.manager = manager; }
    
    public String getParentCode() { return parentCode; }
    public void setParentCode(String parentCode) { this.parentCode = parentCode; }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    
    public Integer getTreeLevel() { return treeLevel; }
    public void setTreeLevel(Integer treeLevel) { this.treeLevel = treeLevel; }

    // camelCase aliases
    public String getCpnyId() { return cpynid; }
    public void setCpnyId(String cpnyId) { this.cpynid = cpnyId; }

    public String getDepNo() { return dep_no; }
    public void setDepNo(String depNo) { this.dep_no = depNo; }

    public String getDepCode() { return dep_code; }
    public void setDepCode(String depCode) { this.dep_code = depCode; }
}