package com.example.cache.example.model;

import java.time.LocalDateTime;
import java.util.Objects;

public class Report {
    private Long id;
    private String title;
    private String content;
    private LocalDateTime generatedAt;
    private Long resourceGroupId;
    private Long resourceSelectorId;
    private Long scheduleId;
    private ReportType type;
    private ReportStatus status;

    public enum ReportType {
        SUMMARY, DETAILED, ANALYTICAL, OPERATIONAL
    }

    public enum ReportStatus {
        GENERATING, COMPLETED, FAILED, CACHED
    }

    // Constructors
    public Report() {}

    public Report(String title, String content, Long resourceGroupId, Long resourceSelectorId, Long scheduleId) {
        this.title = title;
        this.content = content;
        this.resourceGroupId = resourceGroupId;
        this.resourceSelectorId = resourceSelectorId;
        this.scheduleId = scheduleId;
        this.generatedAt = LocalDateTime.now();
        this.type = ReportType.SUMMARY;
        this.status = ReportStatus.GENERATING;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    
    public LocalDateTime getGeneratedAt() { return generatedAt; }
    public void setGeneratedAt(LocalDateTime generatedAt) { this.generatedAt = generatedAt; }
    
    public Long getResourceGroupId() { return resourceGroupId; }
    public void setResourceGroupId(Long resourceGroupId) { this.resourceGroupId = resourceGroupId; }
    
    public Long getResourceSelectorId() { return resourceSelectorId; }
    public void setResourceSelectorId(Long resourceSelectorId) { this.resourceSelectorId = resourceSelectorId; }
    
    public Long getScheduleId() { return scheduleId; }
    public void setScheduleId(Long scheduleId) { this.scheduleId = scheduleId; }
    
    public ReportType getType() { return type; }
    public void setType(ReportType type) { this.type = type; }
    
    public ReportStatus getStatus() { return status; }
    public void setStatus(ReportStatus status) { this.status = status; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Report report = (Report) o;
        return Objects.equals(id, report.id) && Objects.equals(title, report.title);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, title);
    }

    @Override
    public String toString() {
        return String.format("Report{id=%d, title='%s', type=%s, status=%s, generatedAt=%s}", 
                id, title, type, status, generatedAt);
    }
}
