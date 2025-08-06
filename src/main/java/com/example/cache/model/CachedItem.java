package com.example.cache.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;

public class CachedItem<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String primaryKey;
    private final Long longKey;
    private final T value;
    private final List<SearchParameter> parameters;
    private final long ttlMillis;
    private final long createdAt;

    @JsonCreator
    public CachedItem(
            @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("longKey") Long longKey,
            @JsonProperty("value") T value,
            @JsonProperty("parameters") List<SearchParameter> parameters,
            @JsonProperty("ttlMillis") long ttlMillis) {
        this.primaryKey = primaryKey;
        this.longKey = longKey;
        this.value = value;
        this.parameters = parameters;
        this.ttlMillis = ttlMillis;
        this.createdAt = System.currentTimeMillis();
    }

    public String generateUniqueId() {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            StringBuilder sb = new StringBuilder();
            
            if (primaryKey != null) {
                sb.append("key:").append(primaryKey);
            }
            if (longKey != null) {
                sb.append("id:").append(longKey);
            }
            if (parameters != null) {
                parameters.stream()
                    .sorted((a, b) -> Integer.compare(a.getLevel(), b.getLevel()))
                    .forEach(p -> sb.append("param:").append(p.getName()).append("=").append(p.getValue()));
            }
            
            byte[] hash = md.digest(sb.toString().getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    public boolean isExpired() {
        return ttlMillis > 0 && (System.currentTimeMillis() - createdAt) > ttlMillis;
    }

    // Getters
    public String getPrimaryKey() { return primaryKey; }
    public Long getLongKey() { return longKey; }
    public T getValue() { return value; }
    public List<SearchParameter> getParameters() { return parameters; }
    public long getTtlMillis() { return ttlMillis; }
    public long getCreatedAt() { return createdAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CachedItem<?> that = (CachedItem<?>) o;
        return Objects.equals(primaryKey, that.primaryKey) &&
               Objects.equals(longKey, that.longKey) &&
               Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKey, longKey, parameters);
    }
}
