package com.example.cache.example.model;

import jakarta.persistence.*;
import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "resource_groups")
public class ResourceGroup {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(unique = true, nullable = false)
    private String name;
    
    private String description;
    
    @OneToMany(mappedBy = "resourceGroup", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<ResourceSelector> resourceSelectors;

    // Constructors
    public ResourceGroup() {}

    public ResourceGroup(String name, String description) {
        this.name = name;
        this.description = description;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    
    public List<ResourceSelector> getResourceSelectors() { return resourceSelectors; }
    public void setResourceSelectors(List<ResourceSelector> resourceSelectors) { this.resourceSelectors = resourceSelectors; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceGroup that = (ResourceGroup) o;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return String.format("ResourceGroup{id=%d, name='%s', description='%s'}", id, name, description);
    }
}
