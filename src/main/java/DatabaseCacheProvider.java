// DatabaseCacheProvider.java - Oracle database cache provider
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class DatabaseCacheProvider<T> {
    private static final String CREATE_CACHE_TABLE = """
    DECLARE
        table_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO table_exists 
        FROM user_tables 
        WHERE table_name = 'HIERARCHICAL_CACHE';
        
        IF table_exists = 0 THEN
            EXECUTE IMMEDIATE 'CREATE TABLE hierarchical_cache (
                id VARCHAR2(500) PRIMARY KEY NOT NULL,
                string_key VARCHAR2(200),
                long_key NUMBER(19),
                value_data CLOB,
                value_type VARCHAR2(200),
                parameters CLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ttl_millis NUMBER(19),
                expires_at TIMESTAMP
            )';
        END IF;
    END;
    """;

    private static final String CREATE_PARAM_INDEX_TABLE = """
    DECLARE
        table_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO table_exists 
        FROM user_tables 
        WHERE table_name = 'CACHE_PARAM_INDEX';
        
        IF table_exists = 0 THEN
            EXECUTE IMMEDIATE 'CREATE TABLE cache_param_index (
                param_pattern VARCHAR2(1000),
                unique_id VARCHAR2(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (param_pattern, unique_id)
            )';
        END IF;
    END;
    """;
    
    private static final String INSERT_CACHE_ITEM = """
        MERGE INTO hierarchical_cache hc
        USING (SELECT ? as id FROM dual) src
        ON (hc.id = src.id)
        WHEN MATCHED THEN
            UPDATE SET string_key = ?, long_key = ?, value_data = ?, value_type = ?, 
                      parameters = ?, created_at = CURRENT_TIMESTAMP, ttl_millis = ?, 
                      expires_at = CASE WHEN ? > 0 THEN CURRENT_TIMESTAMP + INTERVAL '1' SECOND * (? / 1000) ELSE NULL END
        WHEN NOT MATCHED THEN
            INSERT (id, string_key, long_key, value_data, value_type, parameters, ttl_millis, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CASE WHEN ? > 0 THEN CURRENT_TIMESTAMP + INTERVAL '1' SECOND * (? / 1000) ELSE NULL END)
        """;
    
    private static final String SELECT_BY_ID = """
        SELECT * FROM hierarchical_cache 
        WHERE id = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """;
    
    private static final String SELECT_BY_STRING_KEY = """
        SELECT * FROM hierarchical_cache 
        WHERE string_key = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """;
    
    private static final String SELECT_BY_LONG_KEY = """
        SELECT * FROM hierarchical_cache 
        WHERE long_key = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """;
    
    private static final String DELETE_BY_ID = "DELETE FROM hierarchical_cache WHERE id = ?";
    private static final String DELETE_ALL = "DELETE FROM hierarchical_cache";
    
    private final HikariDataSource dataSource;
    private final ObjectMapper objectMapper;
    
    public DatabaseCacheProvider(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        
        this.dataSource = new HikariDataSource(config);
        this.objectMapper = new ObjectMapper();
        
        initializeTables();
    }
    
    private void initializeTables() {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt1 = conn.prepareStatement(CREATE_CACHE_TABLE);
                 PreparedStatement stmt2 = conn.prepareStatement(CREATE_PARAM_INDEX_TABLE)) {
                stmt1.execute();
                stmt2.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize database tables", e);
        }
    }
    
    public void put(String key, List<SearchParameter> parameters, T value, long ttlMillis, Class<T> valueType) {
        put(key, null, parameters, value, ttlMillis, valueType);
    }
    
    public void put(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis, Class<T> valueType) {
        CachedItem<T> cachedItem = new CachedItem<>(key, id, value, parameters, ttlMillis);
        String uniqueId = cachedItem.generateUniqueId();
        
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            try {
                // Insert/update main cache item
                try (PreparedStatement stmt = conn.prepareStatement(INSERT_CACHE_ITEM)) {
                    String valueData = objectMapper.writeValueAsString(value);
                    String parametersData = objectMapper.writeValueAsString(parameters);
                    
                    stmt.setString(1, uniqueId);
                    stmt.setString(2, key);
                    if (id != null) {
                        stmt.setLong(3, id);
                    } else {
                        stmt.setNull(3, Types.BIGINT);
                    }
                    stmt.setString(4, valueData);
                    stmt.setString(5, valueType.getName());
                    stmt.setString(6, parametersData);
                    stmt.setLong(7, ttlMillis);
                    stmt.setLong(8, ttlMillis);
                    stmt.setLong(9, ttlMillis);
                    stmt.setString(10, uniqueId);
                    stmt.setString(11, key);
                    if (id != null) {
                        stmt.setLong(12, id);
                    } else {
                        stmt.setNull(12, Types.BIGINT);
                    }
                    stmt.setString(13, valueData);
                    stmt.setString(14, valueType.getName());
                    stmt.setString(15, parametersData);
                    stmt.setLong(16, ttlMillis);
                    stmt.setLong(17, ttlMillis);
                    stmt.setLong(18, ttlMillis);
                    
                    stmt.executeUpdate();
                }
                
                // Insert parameter indexes
                Set<String> patterns = generateHierarchicalPatterns(parameters);
                for (String pattern : patterns) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "MERGE INTO cache_param_index cpi USING (SELECT ? as param_pattern, ? as unique_id FROM dual) src " +
                            "ON (cpi.param_pattern = src.param_pattern AND cpi.unique_id = src.unique_id) " +
                            "WHEN NOT MATCHED THEN INSERT (param_pattern, unique_id) VALUES (?, ?)")) {
                        stmt.setString(1, pattern);
                        stmt.setString(2, uniqueId);
                        stmt.setString(3, pattern);
                        stmt.setString(4, uniqueId);
                        stmt.executeUpdate();
                    }
                }
                
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to store item in database", e);
        }
    }
    
    public Optional<T> get(String key, Class<T> valueType) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_BY_STRING_KEY)) {
            
            stmt.setString(1, key);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(deserializeValue(rs.getString("value_data"), valueType));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get item from database", e);
        }
        return Optional.empty();
    }
    
    public Optional<T> get(Long id, Class<T> valueType) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_BY_LONG_KEY)) {
            
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(deserializeValue(rs.getString("value_data"), valueType));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get item from database", e);
        }
        return Optional.empty();
    }
    
    public Optional<T> get(String key, Long id, Class<T> valueType) {
        String uniqueId = key + (id != null ? ":" + id : "");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_BY_ID)) {
            
            stmt.setString(1, uniqueId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(deserializeValue(rs.getString("value_data"), valueType));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get item from database", e);
        }
        return Optional.empty();
    }
    
    public List<T> get(List<SearchParameter> parameters, Class<T> valueType) {
        if (parameters == null || parameters.isEmpty()) {
            return Collections.emptyList();
        }
        
        Set<String> patterns = generateHierarchicalPatterns(parameters);
        Set<String> uniqueIds = new HashSet<>();
        
        try (Connection conn = dataSource.getConnection()) {
            for (String pattern : patterns) {
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT unique_id FROM cache_param_index WHERE param_pattern = ?")) {
                    stmt.setString(1, pattern);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            uniqueIds.add(rs.getString("unique_id"));
                        }
                    }
                }
            }
            
            List<T> results = new ArrayList<>();
            for (String uniqueId : uniqueIds) {
                try (PreparedStatement stmt = conn.prepareStatement(SELECT_BY_ID)) {
                    stmt.setString(1, uniqueId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            results.add(deserializeValue(rs.getString("value_data"), valueType));
                        }
                    }
                }
            }
            return results;
        } catch (Exception e) {
            throw new RuntimeException("Failed to search items in database", e);
        }
    }


    public void invalidate(String key) {
        if (key == null) return;

        try (Connection conn = dataSource.getConnection()) {
            // Start transaction for consistency
            conn.setAutoCommit(false);

            try {
                // First, find associated records to get complete information
                String selectSql = "SELECT string_key, long_key FROM hierarchical_cache WHERE string_key = ?";
                Set<String> keysToRemove = new HashSet<>();
                Set<Long> idsToRemove = new HashSet<>();

                try (PreparedStatement selectStmt = conn.prepareStatement(selectSql)) {
                    selectStmt.setString(1, key);
                    try (ResultSet rs = selectStmt.executeQuery()) {
                        while (rs.next()) {
                            keysToRemove.add(rs.getString("string_key"));
                            Long longKey = rs.getLong("long_key");
                            if (!rs.wasNull()) {
                                idsToRemove.add(longKey);
                            }
                        }
                    }
                }

                // Delete all related entries (cascading)
                String deleteSql = "DELETE FROM hierarchical_cache WHERE string_key = ? OR long_key IN (SELECT long_key FROM hierarchical_cache WHERE string_key = ?)";
                try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                    deleteStmt.setString(1, key);
                    deleteStmt.setString(2, key);
                    int deleted = deleteStmt.executeUpdate();
                    System.out.println("Cascading delete for key '" + key + "': " + deleted + " records removed");
                }

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to invalidate key '" + key + "' in database with cascading", e);
        }
    }

    public void invalidate(Long id) {
        if (id == null) return;

        try (Connection conn = dataSource.getConnection()) {
            // Start transaction for consistency
            conn.setAutoCommit(false);

            try {
                // First, find associated records to get complete information
                String selectSql = "SELECT string_key, long_key FROM hierarchical_cache WHERE long_key = ?";
                Set<String> keysToRemove = new HashSet<>();

                try (PreparedStatement selectStmt = conn.prepareStatement(selectSql)) {
                    selectStmt.setLong(1, id);
                    try (ResultSet rs = selectStmt.executeQuery()) {
                        while (rs.next()) {
                            keysToRemove.add(rs.getString("string_key"));
                        }
                    }
                }

                // Delete all related entries (cascading)
                String deleteSql = "DELETE FROM hierarchical_cache WHERE long_key = ? OR string_key IN (SELECT string_key FROM hierarchical_cache WHERE long_key = ?)";
                try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                    deleteStmt.setLong(1, id);
                    deleteStmt.setLong(2, id);
                    int deleted = deleteStmt.executeUpdate();
                    System.out.println("Cascading delete for ID " + id + ": " + deleted + " records removed");
                }

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to invalidate ID " + id + " in database with cascading", e);
        }
    }

    public void invalidate(String key, Long id) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "DELETE FROM hierarchical_cache WHERE string_key = ? AND long_key = ?")) {
            stmt.setString(1, key);
            stmt.setLong(2, id);
            int deleted = stmt.executeUpdate();
            System.out.println("Deleted " + deleted + " rows for key: " + key + ", ID: " + id);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to invalidate item in database", e);
        }
    }
    public void invalidateAll() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt1 = conn.prepareStatement(DELETE_ALL);
             PreparedStatement stmt2 = conn.prepareStatement("DELETE FROM cache_param_index")) {
            stmt1.executeUpdate();
            stmt2.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to invalidate all items in database", e);
        }
    }
    
    public void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
    
    private T deserializeValue(String valueData, Class<T> valueType) {
        try {
            return objectMapper.readValue(valueData, valueType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize value", e);
        }
    }
    
    private Set<String> generateHierarchicalPatterns(List<SearchParameter> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return Collections.emptySet();
        }
        
        List<SearchParameter> sortedParams = parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::getLevel))
                .collect(Collectors.toList());
        
        Set<String> patterns = new HashSet<>();
        
        // Generate all possible combinations maintaining hierarchy
        for (int i = 0; i < sortedParams.size(); i++) {
            for (int j = i; j < sortedParams.size(); j++) {
                List<SearchParameter> subList = sortedParams.subList(i, j + 1);
                String pattern = subList.stream()
                        .map(SearchParameter::toKey)
                        .collect(Collectors.joining(">"));
                patterns.add(pattern);
            }
        }
        
        // Add individual parameter patterns
        for (SearchParameter param : sortedParams) {
            patterns.add(param.toKey());
        }
        
        return patterns;
    }
}
