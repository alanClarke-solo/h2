package com.example.cache.example.model;

import jakarta.persistence.*;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "resource_selectors")
public class ResourceSelector {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String name;
    
    private String criteria;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "resource_group_id", nullable = false)
    private ResourceGroup resourceGroup;
    
    @OneToMany(mappedBy = "resourceSelector", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Schedule> schedules;

    // Constructors
    public ResourceSelector() {}

    public ResourceSelector(String name, String criteria, ResourceGroup resourceGroup) {
        this.name = name;
        this.criteria = criteria;
        this.resourceGroup = resourceGroup;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getCriteria() { return criteria; }
    public void setCriteria(String criteria) { this.criteria = criteria; }
    
    public ResourceGroup getResourceGroup() { return resourceGroup; }
    public void setResourceGroup(ResourceGroup resourceGroup) { this.resourceGroup = resourceGroup; }
    
    public List<Schedule> getSchedules() { return schedules; }
    public void setSchedules(List<Schedule> schedules) { this.schedules = schedules; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceSelector that = (ResourceSelector) o;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return String.format("ResourceSelector{id=%d, name='%s', criteria='%s'}", id, name, criteria);
    }
}
