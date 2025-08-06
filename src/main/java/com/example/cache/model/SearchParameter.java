package com.example.cache.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class SearchParameter implements Serializable, Comparable<SearchParameter> {
    private static final long serialVersionUID = 1L;
    
    private final String name;
    private final String value;
    private final int level;

    @JsonCreator
    public SearchParameter(
            @JsonProperty("name") String name,
            @JsonProperty("value") String value,
            @JsonProperty("level") int level) {
        this.name = name;
        this.value = value;
        this.level = level;
    }

    public String getName() { return name; }
    public String getValue() { return value; }
    public int getLevel() { return level; }

    @Override
    public int compareTo(SearchParameter other) {
        return Integer.compare(this.level, other.level);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchParameter that = (SearchParameter) o;
        return level == that.level &&
               Objects.equals(name, that.name) &&
               Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, level);
    }

    @Override
    public String toString() {
        return String.format("SearchParameter{name='%s', value='%s', level=%d}", name, value, level);
    }
}
