package com.h2.spring.cache.model;

import java.io.Serializable;
import java.util.List;

public class UserSummary implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String department;
    private int totalUsers;
    private int activeUsers;
    private List<String> roles;

    public UserSummary() {}

    public UserSummary(String department, int totalUsers, int activeUsers, List<String> roles) {
        this.department = department;
        this.totalUsers = totalUsers;
        this.activeUsers = activeUsers;
        this.roles = roles;
    }

    // Getters and setters
    public String getDepartment() { return department; }
    public void setDepartment(String department) { this.department = department; }

    public int getTotalUsers() { return totalUsers; }
    public void setTotalUsers(int totalUsers) { this.totalUsers = totalUsers; }

    public int getActiveUsers() { return activeUsers; }
    public void setActiveUsers(int activeUsers) { this.activeUsers = activeUsers; }

    public List<String> getRoles() { return roles; }
    public void setRoles(List<String> roles) { this.roles = roles; }
}
