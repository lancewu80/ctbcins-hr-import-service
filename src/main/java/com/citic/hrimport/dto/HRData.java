package com.citic.hrimport.dto;

import com.opencsv.bean.CsvBindByName;

public class HRData {
    @CsvBindByName(column = "CPNYID")
    private String cpnyId;
    
    @CsvBindByName(column = "DEP_NO")
    private String depNo;
    
    @CsvBindByName(column = "DEP_CODE")
    private String depCode;
    
    @CsvBindByName(column = "DEP_NAME")
    private String depName;
    
    @CsvBindByName(column = "STATE_NO")
    private String stateNo;
    
    @CsvBindByName(column = "STATE_NAME")
    private String stateName;
    
    @CsvBindByName(column = "EMP_ID")
    private String empId;
    
    @CsvBindByName(column = "EMP_NAME")
    private String empName;
    
    @CsvBindByName(column = "WORKCARD")
    private String workcard;
    
    @CsvBindByName(column = "INADATE")
    private String inaDate;
    
    @CsvBindByName(column = "QUITDATE")
    private String quitDate;
    
    @CsvBindByName(column = "STOP_W")
    private String stopW;
    
    @CsvBindByName(column = "START_W")
    private String startW;
    
    @CsvBindByName(column = "MDATE")
    private String mdate;
    
    @CsvBindByName(column = "POSITION_NAME")
    private String positionName;
    
    @CsvBindByName(column = "MOBILE")
    private String mobile;
    
    @CsvBindByName(column = "TITLE_NAME")
    private String titleName;
    
    @CsvBindByName(column = "WORKPLACE_NAME")
    private String workplaceName;

    // getters and setters
    public String getCpnyId() { return cpnyId; }
    public void setCpnyId(String cpnyId) { this.cpnyId = cpnyId; }
    
    public String getDepNo() { return depNo; }
    public void setDepNo(String depNo) { this.depNo = depNo; }
    
    public String getDepCode() { return depCode; }
    public void setDepCode(String depCode) { this.depCode = depCode; }
    
    public String getDepName() { return depName; }
    public void setDepName(String depName) { this.depName = depName; }
    
    public String getStateNo() { return stateNo; }
    public void setStateNo(String stateNo) { this.stateNo = stateNo; }
    
    public String getStateName() { return stateName; }
    public void setStateName(String stateName) { this.stateName = stateName; }
    
    public String getEmpId() { return empId; }
    public void setEmpId(String empId) { this.empId = empId; }
    
    public String getEmpName() { return empName; }
    public void setEmpName(String empName) { this.empName = empName; }
    
    public String getWorkcard() { return workcard; }
    public void setWorkcard(String workcard) { this.workcard = workcard; }
    
    public String getInaDate() { return inaDate; }
    public void setInaDate(String inaDate) { this.inaDate = inaDate; }
    
    public String getQuitDate() { return quitDate; }
    public void setQuitDate(String quitDate) { this.quitDate = quitDate; }
    
    public String getStopW() { return stopW; }
    public void setStopW(String stopW) { this.stopW = stopW; }
    
    public String getStartW() { return startW; }
    public void setStartW(String startW) { this.startW = startW; }
    
    public String getMdate() { return mdate; }
    public void setMdate(String mdate) { this.mdate = mdate; }
    
    public String getPositionName() { return positionName; }
    public void setPositionName(String positionName) { this.positionName = positionName; }
    
    public String getMobile() { return mobile; }
    public void setMobile(String mobile) { this.mobile = mobile; }
    
    public String getTitleName() { return titleName; }
    public void setTitleName(String titleName) { this.titleName = titleName; }
    
    public String getWorkplaceName() { return workplaceName; }
    public void setWorkplaceName(String workplaceName) { this.workplaceName = workplaceName; }

    @Override
    public String toString() {
        return "HRData{" +
                "empName='" + empName + '\'' +
                ", workcard='" + workcard + '\'' +
                ", depName='" + depName + '\'' +
                ", stateNo='" + stateNo + '\'' +
                '}';
    }
}