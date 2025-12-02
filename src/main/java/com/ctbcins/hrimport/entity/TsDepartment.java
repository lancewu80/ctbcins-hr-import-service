package com.ctbcins.hrimport.entity;

import javax.persistence.*;
import java.util.UUID;

@Entity
@Table(name = "TsDepartment")
public class TsDepartment {

    @Id
    @Column(name = "FId", columnDefinition = "uniqueidentifier")
    private UUID FId;

    @Column(name = "FParentId", columnDefinition = "uniqueidentifier")
    private UUID FParentId;

    @Column(name = "FIndex")
    private Integer FIndex;

    @Column(name = "FTreeLevel")
    private Integer FTreeLevel;

    @Column(name = "FTreeSerial", length = 100, columnDefinition = "nvarchar(100)")
    private String FTreeSerial;

    @Column(name = "FName", length = 50, columnDefinition = "nvarchar(50)")
    private String FName;

    @Column(name = "FFullName", length = 100, columnDefinition = "nvarchar(100)")
    private String FFullName;

    @Column(name = "FShortCode", length = 50, columnDefinition = "nvarchar(50)")
    private String FShortCode;

    @Column(name = "FUserId", columnDefinition = "uniqueidentifier")
    private UUID FUserId;

    @Column(name = "FEnabled")
    private Boolean FEnabled = true; // table default ((1))

    @Column(name = "FIsCompany")
    private Boolean FIsCompany = false; // table default ((0))

    @Column(name = "FDescription", length = 500, columnDefinition = "nvarchar(500)")
    private String FDescription;

    @Column(name = "FIsServices")
    private Boolean FIsServices;

    @Column(name = "FIsSales")
    private Boolean FIsSales;

    // getters & setters

    public UUID getFId() {
        return FId;
    }

    public void setFId(UUID fId) {
        FId = fId;
    }

    public UUID getFParentId() {
        return FParentId;
    }

    public void setFParentId(UUID fParentId) {
        FParentId = fParentId;
    }

    public Integer getFIndex() {
        return FIndex;
    }

    public void setFIndex(Integer FIndex) {
        this.FIndex = FIndex;
    }

    public Integer getFTreeLevel() {
        return FTreeLevel;
    }

    public void setFTreeLevel(Integer FTreeLevel) {
        this.FTreeLevel = FTreeLevel;
    }

    public String getFTreeSerial() {
        return FTreeSerial;
    }

    public void setFTreeSerial(String FTreeSerial) {
        this.FTreeSerial = FTreeSerial;
    }

    public String getFName() {
        return FName;
    }

    public void setFName(String FName) {
        this.FName = FName;
    }

    public String getFFullName() {
        return FFullName;
    }

    public void setFFullName(String FFullName) {
        this.FFullName = FFullName;
    }

    public String getFShortCode() {
        return FShortCode;
    }

    public void setFShortCode(String FShortCode) {
        this.FShortCode = FShortCode;
    }

    public UUID getFUserId() {
        return FUserId;
    }

    public void setFUserId(UUID FUserId) {
        this.FUserId = FUserId;
    }

    public Boolean getFEnabled() {
        return FEnabled;
    }

    public void setFEnabled(Boolean FEnabled) {
        this.FEnabled = FEnabled;
    }

    public Boolean getFIsCompany() {
        return FIsCompany;
    }

    public void setFIsCompany(Boolean FIsCompany) {
        this.FIsCompany = FIsCompany;
    }

    public String getFDescription() {
        return FDescription;
    }

    public void setFDescription(String FDescription) {
        this.FDescription = FDescription;
    }

    public Boolean getFIsServices() {
        return FIsServices;
    }

    public void setFIsServices(Boolean FIsServices) {
        this.FIsServices = FIsServices;
    }

    public Boolean getFIsSales() {
        return FIsSales;
    }

    public void setFIsSales(Boolean FIsSales) {
        this.FIsSales = FIsSales;
    }
}
