// SchemaPrefixedNamingStrategy.java
package ac.h2.config;

import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

public class SchemaPrefixedNamingStrategy implements NamingStrategy {

    private final String schema; // e.g. "CACHE_SCHEMA"

    public SchemaPrefixedNamingStrategy(String schema) {
        this.schema = (schema == null || schema.isBlank()) ? null : schema.trim().toUpperCase();
    }

    @Override
    public String getTableName(Class<?> type) {
        String table = NamingStrategy.super.getTableName(type).toUpperCase();
        return schema == null ? table : schema + "." + table;
    }

    @Override
    public String getColumnName(RelationalPersistentProperty property) {
        return NamingStrategy.super.getColumnName(property).toUpperCase();
    }

    @Override
    public String getKeyColumn(RelationalPersistentProperty property) {
        return NamingStrategy.super.getKeyColumn(property).toUpperCase();
    }

    @Override
    public String getReverseColumnName(RelationalPersistentProperty property) {
        return NamingStrategy.super.getReverseColumnName(property).toUpperCase();
    }
}
