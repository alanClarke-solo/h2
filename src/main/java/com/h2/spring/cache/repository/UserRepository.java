package com.h2.spring.cache.repository;

import com.h2.spring.cache.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    
    Optional<User> findByUsername(String username);
    
    Optional<User> findByEmail(String email);
    
    List<User> findByDepartment(String department);
    
    List<User> findByRole(String role);
    
    List<User> findByDepartmentAndRole(String department, String role);
    
    List<User> findByActiveTrue();
    
    @Query("SELECT u FROM User u WHERE u.department = :department AND u.active = true")
    List<User> findActiveUsersByDepartment(@Param("department") String department);
}
