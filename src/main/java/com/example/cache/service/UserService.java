// UserService.java (Enhanced with both standard and hierarchical caching)
package com.example.cache.service;

import ac.h2.SearchParameter;
import com.example.cache.annotation.HierarchicalCacheable;
import com.example.cache.model.User;
import com.example.cache.model.UserSummary;
import com.example.cache.repository.UserRepository;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserService {

    private final UserRepository userRepository;
    private final HierarchicalCacheService cacheService;

    public UserService(UserRepository userRepository, HierarchicalCacheService cacheService) {
        this.userRepository = userRepository;
        this.cacheService = cacheService;
    }

    // STANDARD Spring Cache Annotations (will use HierarchicalCacheManager)

    @Cacheable(value = "users", key = "#id")
    public User findById(Long id) {
        return userRepository.findById(id).orElse(null);
    }

    @Cacheable(value = "users", key = "'username:' + #username")
    public User findByUsername(String username) {
        return userRepository.findByUsername(username).orElse(null);
    }

    @Cacheable(value = "user-lists", key = "'department:' + #department")
    public List<User> findByDepartment(String department) {
        return userRepository.findByDepartment(department);
    }

    @CacheEvict(value = "users", key = "#user.id")
    public User saveUser(User user) {
        return userRepository.save(user);
    }

    @Caching(evict = {
            @CacheEvict(value = "users", key = "#id"),
            @CacheEvict(value = "user-lists", allEntries = true)
    })
    public void deleteUser(Long id) {
        userRepository.deleteById(id);
    }

    // HIERARCHICAL Cache Annotations (custom functionality)

    @HierarchicalCacheable(
            value = "hierarchical-users",
            key = "'user:' + #userId",
            parameters = {"#department", "#role"},
            levels = {1, 2},
            cacheLevel = "level_1",
            ttlMinutes = 30
    )
    public User findUserWithHierarchy(Long userId, String department, String role) {
        User user = userRepository.findById(userId).orElse(null);
        // Validate that user matches the department and role criteria
        if (user != null &&
                (department == null || department.equals(user.getDepartment())) &&
                (role == null || role.equals(user.getRole()))) {
            return user;
        }
        return null;
    }

    @HierarchicalCacheable(
            value = "hierarchical-user-lists",
            key = "'users:' + #department + ':' + #role",
            parameters = {"#department", "#role", "#active"},
            levels = {1, 2, 3},
            cacheLevel = "level_2",
            ttlMinutes = 15
    )
    public List<User> findUsersWithHierarchy(String department, String role, Boolean active) {
        if (department != null && role != null && active != null) {
            return userRepository.findByDepartmentAndRole(department, role)
                    .stream()
                    .filter(user -> user.isActive() == active)
                    .toList();
        } else if (department != null && active != null) {
            return userRepository.findByDepartment(department)
                    .stream()
                    .filter(user -> user.isActive() == active)
                    .toList();
        } else if (department != null) {
            return userRepository.findByDepartment(department);
        }
        return userRepository.findAll();
    }

    // PROGRAMMATIC Hierarchical Caching (direct API usage)

    public List<User> findUsersByDepartmentProgrammatic(String department) {
        List<SearchParameter> params = List.of(
                new SearchParameter("department", department, 1)
        );

        return cacheService.getOrCompute(
                "users:department:" + department,
                (Class<List<User>>) (Class<?>) List.class,
                () -> userRepository.findByDepartment(department),
                "level_2"
        );
    }

    public List<User> findUsersByComplexCriteria(String department, String role, Boolean active) {
        List<SearchParameter> params = List.of(
                new SearchParameter("department", department, 1),
                new SearchParameter("role", role, 2),
                new SearchParameter("active", active != null ? active.toString() : "any", 3)
        );

        // Try to get from cache with hierarchical search
        List<User> cachedResult = cacheService.get(params, User.class, "level_2");

        if (!cachedResult.isEmpty()) {
            return cachedResult;
        }

        // Fetch from database
        List<User> result;
        if (department != null && role != null && active != null) {
            result = userRepository.findByDepartmentAndRole(department, role)
                    .stream()
                    .filter(user -> user.isActive() == active)
                    .toList();
        } else if (department != null) {
            result = userRepository.findByDepartment(department);
        } else {
            result = userRepository.findAll();
        }

        // Cache the result
        if (!result.isEmpty()) {
            String cacheKey = "users:complex:" + department + ":" + role + ":" + active;
            cacheService.put(cacheKey, params, result, "level_2");
        }

        return result;
    }

    // MIXED Usage: Standard + Hierarchical

    @Cacheable(value = "user-summaries", key = "'summary:' + #department")
    public UserSummary getDepartmentSummary(String department) {
        // Use hierarchical cache for detailed data
        List<User> users = findUsersByDepartmentProgrammatic(department);

        return new UserSummary(
                department,
                users.size(),
                (int) users.stream().filter(User::isActive).count(),
                users.stream().map(User::getRole).distinct().toList()
        );
    }

    // Cache invalidation methods

    public void invalidateUserCache(Long userId) {
        // Standard cache eviction
        // This will be handled by @CacheEvict annotations

        // Hierarchical cache eviction
        cacheService.invalidate("user:" + userId, "level_1");
    }

    public void invalidateDepartmentCache(String department) {
        cacheService.invalidate("department", "level_1");
    }
}