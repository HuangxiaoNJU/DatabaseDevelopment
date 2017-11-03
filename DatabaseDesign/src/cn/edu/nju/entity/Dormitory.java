package cn.edu.nju.entity;

public class Dormitory {

    private String dormitoryName;
    private String campus;
    private String phoneNumber;
    private int standardCost;

    public String getDormitoryName() {
        return dormitoryName;
    }

    public void setDormitoryName(String dormitoryName) {
        this.dormitoryName = dormitoryName;
    }

    public String getCampus() {
        return campus;
    }

    public void setCampus(String campus) {
        this.campus = campus;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public int getStandardCost() {
        return standardCost;
    }

    public void setStandardCost(int standardCost) {
        this.standardCost = standardCost;
    }
}
