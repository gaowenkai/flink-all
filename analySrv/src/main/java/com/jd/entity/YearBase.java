package com.jd.entity;

public class YearBase {

    private String yearType;
    private Long count;
    private String groupFiled;


    public String getYearType() {
        return yearType;
    }

    public Long getCount() {
        return count;
    }

    public String getGroupFiled() {
        return groupFiled;
    }

    public void setYearType(String yearType) {
        this.yearType = yearType;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public void setGroupFiled(String groupFiled) {
        this.groupFiled = groupFiled;
    }
}
