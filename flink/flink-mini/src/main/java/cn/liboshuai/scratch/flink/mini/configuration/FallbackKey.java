package cn.liboshuai.scratch.flink.mini.configuration;

import lombok.Getter;

@Getter
public class FallbackKey {

    static FallbackKey createFallbackKey(String key) {
        return new FallbackKey(key, true);
    }

    static FallbackKey createDeprecatedKey(String key) {
        return new FallbackKey(key, false);
    }

    private final String key;
    private final boolean isDeprecated;

    private FallbackKey(String key, boolean isDeprecated) {
        this.key = key;
        this.isDeprecated = isDeprecated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == FallbackKey.class) {
            FallbackKey that = (FallbackKey) o;
            return this.key.equals(that.key) && (this.isDeprecated == that.isDeprecated);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + (isDeprecated ? 1 : 0);
    }

    @Override
    public String toString() {
        return String.format("{key=%s, isDeprecated=%s}", key, isDeprecated);
    }
}
