package ac.h2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// CachedItem.java
public class CachedItem<T> implements Serializable {
    private final String stringKey;
    private final Long longKey;
    private final T value;
    private final List<SearchParameter> parameters;
    private final long createdAt;
    private final long ttl;

    public CachedItem(String stringKey, Long longKey, T value, List<SearchParameter> parameters, long ttl) {
        this.stringKey = stringKey;
        this.longKey = longKey;
        this.value = value;
        this.parameters = new ArrayList<>(parameters);
        this.createdAt = System.currentTimeMillis();
        this.ttl = ttl;
    }

    public String getStringKey() {
        return stringKey;
    }

    public Long getLongKey() {
        return longKey;
    }

    public T getValue() {
        return value;
    }

    public List<SearchParameter> getParameters() {
        return new ArrayList<>(parameters);
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getTtl() {
        return ttl;
    }

    public boolean isExpired() {
        return ttl > 0 && (System.currentTimeMillis() - createdAt) > ttl;
    }

    public String generateUniqueId() {
        return stringKey + (longKey != null ? ":" + longKey : "");
    }
}
