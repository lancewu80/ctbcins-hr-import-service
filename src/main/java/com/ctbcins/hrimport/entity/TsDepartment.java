package com.ctbcins.hrimport.entity;

import javax.persistence.*;
import java.math.BigDecimal;

@Entity
@Table(name = "TsDepartment")
public class TsDepartment {

    @Id
    @Column(name = "FId", length = 36)
    private String FId;

    @Column(name = "FParentId", length = 36)
    private String FParentId;

    @Column(name = "FIndex")
    private Integer FIndex;

    @Column(name = "FTreeLevel")
    private Integer FTreeLevel;

    @Column(name = "FTreeSerial", length = 100)
    private String FTreeSerial;

    @Column(name = "FName", length = 50)
    private String FName;

    @Column(name = "FFullName", length = 100)
    private String FFullName;

    @Column(name = "FShortCode", length = 50)
    private String FShortCode;

    @Column(name = "FUserId", length = 36)
    private String FUserId;

    // Database stores these as numeric; map as BigDecimal to match Postgres NUMERIC
    @Column(name = "FEnabled")
    private BigDecimal FEnabled = BigDecimal.ONE; // default 1

    @Column(name = "FIsCompany")
    private BigDecimal FIsCompany = BigDecimal.ZERO; // default 0

    @Column(name = "FDescription", length = 500)
    private String FDescription;

    @Column(name = "FIsServices")
    private BigDecimal FIsServices;

    @Column(name = "FIsSales")
    private BigDecimal FIsSales;

    // getters & setters

    public String getFId() {
        return FId;
    }

    public void setFId(String fId) {
        FId = fId;
    }

    public String getFParentId() {
        return FParentId;
    }

    public void setFParentId(String fParentId) {
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

    public String getFUserId() {
        return FUserId;
    }

    public void setFUserId(String FUserId) {
        this.FUserId = FUserId;
    }

    // Keep public getter/setter signatures as Boolean for compatibility, but convert to/from BigDecimal for DB mapping
    public Boolean getFEnabled() {
        return FEnabled != null && FEnabled.intValue() != 0;
    }

    public void setFEnabled(Boolean FEnabled) {
        this.FEnabled = (FEnabled == null) ? null : (FEnabled ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public Boolean getFIsCompany() {
        return FIsCompany != null && FIsCompany.intValue() != 0;
    }

    public void setFIsCompany(Boolean FIsCompany) {
        this.FIsCompany = (FIsCompany == null) ? null : (FIsCompany ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public String getFDescription() {
        return FDescription;
    }

    public void setFDescription(String FDescription) {
        this.FDescription = FDescription;
    }

    public Boolean getFIsServices() {
        return FIsServices != null && FIsServices.intValue() != 0;
    }

    public void setFIsServices(Boolean FIsServices) {
        this.FIsServices = (FIsServices == null) ? null : (FIsServices ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    public Boolean getFIsSales() {
        return FIsSales != null && FIsSales.intValue() != 0;
    }

    public void setFIsSales(Boolean FIsSales) {
        this.FIsSales = (FIsSales == null) ? null : (FIsSales ? BigDecimal.ONE : BigDecimal.ZERO);
    }
}
