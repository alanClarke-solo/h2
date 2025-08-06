package com.example.cache.example.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.Objects;

@Entity
@Table(name = "schedules")
public class Schedule {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String name;
    
    @Column(name = "cron_expression")
    private String cronExpression;
    
    @Column(name = "start_time")
    private LocalDateTime startTime;
    
    @Column(name = "end_time")
    private LocalDateTime endTime;
    
    @Enumerated(EnumType.STRING)
    private ScheduleStatus status;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "resource_selector_id", nullable = false)
    private ResourceSelector resourceSelector;

    public enum ScheduleStatus {
        ACTIVE, INACTIVE, SUSPENDED
    }

    // Constructors
    public Schedule() {}

    public Schedule(String name, String cronExpression, ResourceSelector resourceSelector) {
        this.name = name;
        this.cronExpression = cronExpression;
        this.resourceSelector = resourceSelector;
        this.status = ScheduleStatus.ACTIVE;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getCronExpression() { return cronExpression; }
    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }
    
    public LocalDateTime getStartTime() { return startTime; }
    public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }
    
    public LocalDateTime getEndTime() { return endTime; }
    public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }
    
    public ScheduleStatus getStatus() { return status; }
    public void setStatus(ScheduleStatus status) { this.status = status; }
    
    public ResourceSelector getResourceSelector() { return resourceSelector; }
    public void setResourceSelector(ResourceSelector resourceSelector) { this.resourceSelector = resourceSelector; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schedule schedule = (Schedule) o;
        return Objects.equals(id, schedule.id) && Objects.equals(name, schedule.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return String.format("Schedule{id=%d, name='%s', cronExpression='%s', status=%s}", 
                id, name, cronExpression, status);
    }
}
