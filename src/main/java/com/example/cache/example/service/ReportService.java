package com.example.cache.example.service;

import ac.h2.CacheStatistics;
import com.example.cache.example.model.Report;
import com.example.cache.model.SearchParameter;
import com.example.cache.service.TransparentHierarchicalCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Service
public class ReportService {
    private static final Logger logger = LoggerFactory.getLogger(ReportService.class);
    
    private final TransparentHierarchicalCacheService<Report> cacheService;

    public ReportService(TransparentHierarchicalCacheService<Report> cacheService) {
        this.cacheService = cacheService;
    }

    /**
     * Generate report using hierarchical cache
     * Cache levels: ResourceGroup (1) -> ResourceSelector (2) -> Schedule (3)
     */
    public Report generateReport(Long resourceGroupId, Long resourceSelectorId, Long scheduleId, Report.ReportType type) {
        // Create hierarchical search parameters
        List<SearchParameter> parameters = Arrays.asList(
                new SearchParameter("resourceGroup", resourceGroupId.toString(), 1),
                new SearchParameter("resourceSelector", resourceSelectorId.toString(), 2),
                new SearchParameter("schedule", scheduleId.toString(), 3),
                new SearchParameter("type", type.toString(), 4)
        );

        String cacheKey = String.format("report:%d:%d:%d:%s", resourceGroupId, resourceSelectorId, scheduleId, type);

        // Try to get from cache or compute
        return cacheService.getOrCompute(cacheKey, Report.class, () -> {
            logger.info("Generating new report for ResourceGroup={}, ResourceSelector={}, Schedule={}, Type={}", 
                    resourceGroupId, resourceSelectorId, scheduleId, type);
            
            // Simulate report generation
            Report report = new Report();
            report.setId(System.currentTimeMillis());
            report.setTitle(String.format("Report for RG:%d RS:%d S:%d", resourceGroupId, resourceSelectorId, scheduleId));
            report.setContent(generateReportContent(resourceGroupId, resourceSelectorId, scheduleId, type));
            report.setResourceGroupId(resourceGroupId);
            report.setResourceSelectorId(resourceSelectorId);
            report.setScheduleId(scheduleId);
            report.setType(type);
            report.setStatus(Report.ReportStatus.COMPLETED);
            report.setGeneratedAt(LocalDateTime.now());

            return report;
        });
    }

    /**
     * Get reports for a resource group (hierarchical search)
     */
    public List<Report> getReportsForResourceGroup(Long resourceGroupId) {
        List<SearchParameter> parameters = Arrays.asList(
                new SearchParameter("resourceGroup", resourceGroupId.toString(), 1)
        );

        return cacheService.get(parameters, Report.class);
    }

    /**
     * Get reports for a resource selector (hierarchical search)
     */
    public List<Report> getReportsForResourceSelector(Long resourceGroupId, Long resourceSelectorId) {
        List<SearchParameter> parameters = Arrays.asList(
                new SearchParameter("resourceGroup", resourceGroupId.toString(), 1),
                new SearchParameter("resourceSelector", resourceSelectorId.toString(), 2)
        );

        return cacheService.get(parameters, Report.class);
    }

    /**
     * Get reports for a specific schedule (hierarchical search)
     */
    public List<Report> getReportsForSchedule(Long resourceGroupId, Long resourceSelectorId, Long scheduleId) {
        List<SearchParameter> parameters = Arrays.asList(
                new SearchParameter("resourceGroup", resourceGroupId.toString(), 1),
                new SearchParameter("resourceSelector", resourceSelectorId.toString(), 2),
                new SearchParameter("schedule", scheduleId.toString(), 3)
        );

        return cacheService.get(parameters, Report.class);
    }

    /**
     * Using standard Spring Cache annotations (alternative approach)
     */
    @Cacheable(value = "reports", key = "#resourceGroupId + ':' + #resourceSelectorId + ':' + #scheduleId + ':' + #type")
    public Report generateReportWithSpringCache(Long resourceGroupId, Long resourceSelectorId, Long scheduleId, Report.ReportType type) {
        logger.info("Generating report with Spring Cache for ResourceGroup={}, ResourceSelector={}, Schedule={}, Type={}", 
                resourceGroupId, resourceSelectorId, scheduleId, type);
        
        Report report = new Report();
        report.setId(System.currentTimeMillis());
        report.setTitle(String.format("Cached Report for RG:%d RS:%d S:%d", resourceGroupId, resourceSelectorId, scheduleId));
        report.setContent(generateReportContent(resourceGroupId, resourceSelectorId, scheduleId, type));
        report.setResourceGroupId(resourceGroupId);
        report.setResourceSelectorId(resourceSelectorId);
        report.setScheduleId(scheduleId);
        report.setType(type);
        report.setStatus(Report.ReportStatus.CACHED);
        report.setGeneratedAt(LocalDateTime.now());

        return report;
    }

    /**
     * Invalidate cache for specific resource group
     */
    public void invalidateResourceGroupReports(Long resourceGroupId) {
        List<SearchParameter> parameters = Arrays.asList(
                new SearchParameter("resourceGroup", resourceGroupId.toString(), 1)
        );
        cacheService.invalidateByPattern(parameters);
        logger.info("Invalidated cache for ResourceGroup={}", resourceGroupId);
    }

    /**
     * Invalidate cache for specific resource selector
     */
    public void invalidateResourceSelectorReports(Long resourceGroupId, Long resourceSelectorId) {
        List<SearchParameter> parameters = Arrays.asList(
                new SearchParameter("resourceGroup", resourceGroupId.toString(), 1),
                new SearchParameter("resourceSelector", resourceSelectorId.toString(), 2)
        );
        cacheService.invalidateByPattern(parameters);
        logger.info("Invalidated cache for ResourceGroup={}, ResourceSelector={}", resourceGroupId, resourceSelectorId);
    }

    /**
     * Invalidate cache for specific schedule
     */
    public void invalidateScheduleReports(Long resourceGroupId, Long resourceSelectorId, Long scheduleId) {
        List<SearchParameter> parameters = Arrays.asList(
                new SearchParameter("resourceGroup", resourceGroupId.toString(), 1),
                new SearchParameter("resourceSelector", resourceSelectorId.toString(), 2),
                new SearchParameter("schedule", scheduleId.toString(), 3)
        );
        cacheService.invalidateByPattern(parameters);
        logger.info("Invalidated cache for ResourceGroup={}, ResourceSelector={}, Schedule={}", 
                resourceGroupId, resourceSelectorId, scheduleId);
    }

    /**
     * Get specific report by ID
     */
    public Optional<Report> getReportById(Long reportId) {
        String cacheKey = "report_by_id:" + reportId;
        return Optional.ofNullable(cacheService.getOrCompute(cacheKey, Report.class, () -> {
            // Simulate database lookup
            logger.info("Fetching report by ID: {}", reportId);
            // Return null if not found, or the actual report
            return null; // Replace with actual database lookup
        }));
    }

    private String generateReportContent(Long resourceGroupId, Long resourceSelectorId, Long scheduleId, Report.ReportType type) {
        // Simulate complex report generation logic
        try {
            Thread.sleep(100); // Simulate processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return String.format("""
                %s Report Generated
                ==================
                Resource Group ID: %d
                Resource Selector ID: %d
                Schedule ID: %d
                Generated at: %s
                
                Report content would contain detailed analysis and data...
                """, type, resourceGroupId, resourceSelectorId, scheduleId, LocalDateTime.now());
    }

    /**
     * Get cache statistics
     */
/*
    public String getCacheStatistics() {
        CacheStatistics stats = cacheService.getStatistics();
        return String.format("""
                Cache Statistics:
                Requests: %d
                Hits: %d
                Misses: %d
                Hit Rate: %.2f%%
                Values: %d
                Keys: %d
                """, 
                stats.getRequests(), 
                stats.getHits(), 
                stats.getMisses(),
                stats.getHitRate() * 100,
                stats.getValues(),
                stats.getKeys());
    }
*/
}
