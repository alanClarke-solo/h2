package ac.h2;

import java.util.Objects;

// SearchParameter.java
public class SearchParameter {
    private final String name;
    private final String value;
    private final int level;

    public SearchParameter(String name, String value, int level) {
        this.name = Objects.requireNonNull(name, "Name cannot be null");
        this.value = Objects.requireNonNull(value, "Value cannot be null");
        this.level = level;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public int getLevel() {
        return level;
    }

    public String toKey() {
        return "L" + level + ":" + name + "=" + value;
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
        return toKey();
    }
}
