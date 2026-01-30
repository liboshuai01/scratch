package cn.liboshuai.scratch.flink.mini.configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigOption<T> {
    private final String key;
    private final FallbackKey[] fallbackKeys;
    private final T defaultValue;
    private final String description;
    private final Class<?> clazz;
    private final boolean isList;

    private static final FallbackKey[] EMPTY = new FallbackKey[0];

    ConfigOption(String key, T defaultValue, String description, Class<?> clazz, boolean isList, FallbackKey... fallbackKeys) {
        this.key = key;
        this.fallbackKeys = (fallbackKeys == null || fallbackKeys.length == 0) ? EMPTY : fallbackKeys;
        this.defaultValue = defaultValue;
        this.description = description;
        this.clazz = clazz;
        this.isList = isList;
    }

    Class<?> getClazz() {
        return clazz;
    }

    boolean isList() {
        return isList;
    }


    public ConfigOption<T> withFallbackKeys(final String... fallbackKeys) {
        final Stream<FallbackKey> newFallbackKeys = Arrays.stream(fallbackKeys).map(FallbackKey::createFallbackKey);
        final Stream<FallbackKey> currentFallbackKeys = Arrays.stream(this.fallbackKeys);
        final FallbackKey[] mergedAlternativeKeys = Stream.concat(newFallbackKeys, currentFallbackKeys).toArray(FallbackKey[]::new);
        return new ConfigOption<>(key, defaultValue, description, clazz, isList, mergedAlternativeKeys);
    }

    public ConfigOption<T> withDeprecatedKeys(final String... deprecatedKeys) {
        final Stream<FallbackKey> newDeprecatedKeys = Arrays.stream(deprecatedKeys).map(FallbackKey::createDeprecatedKey);
        final Stream<FallbackKey> currentDeprecatedKeys = Arrays.stream(this.fallbackKeys);
        FallbackKey[] mergedAlternativeKeys = Stream.concat(newDeprecatedKeys, currentDeprecatedKeys).toArray(FallbackKey[]::new);
        return new ConfigOption<>(key, defaultValue, description, clazz, isList, mergedAlternativeKeys);
    }

    public ConfigOption<T> withDescription(final String description) {
        return new ConfigOption<>(key, defaultValue, description, clazz, isList, fallbackKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ConfigOption<?> that = (ConfigOption<?>) o;
        return isList == that.isList && Objects.equals(key, that.key) && Objects.deepEquals(fallbackKeys, that.fallbackKeys) && Objects.equals(defaultValue, that.defaultValue) && Objects.equals(description, that.description) && Objects.equals(clazz, that.clazz);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode()
                + 17 * Arrays.hashCode(fallbackKeys)
                + (defaultValue != null ? defaultValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return String.format(
                "Key: '%s' , default: %s (fallback keys: %s)",
                key, defaultValue, Arrays.toString(fallbackKeys));
    }

    public String key() {
        return this.key;
    }

    public boolean hasDefaultValue() {
        return this.defaultValue != null;
    }

    public String description() {
        return this.description;
    }

    public Iterable<FallbackKey> fallbackKeys() {
        return (fallbackKeys == EMPTY) ? Collections.emptyList() : Arrays.asList(fallbackKeys);
    }

    public boolean hasFallbackKeys() {
        return fallbackKeys != EMPTY;
    }

    public T defaultValue() {
        return defaultValue;
    }

    @Deprecated
    public boolean hasDeprecatedKeys() {
        return fallbackKeys != EMPTY
                && Arrays.stream(fallbackKeys).anyMatch(FallbackKey::isDeprecated);
    }

    @Deprecated
    public Iterable<String> deprecatedKeys() {
        return fallbackKeys == EMPTY
                ? Collections.emptyList()
                : Arrays.stream(fallbackKeys)
                .filter(FallbackKey::isDeprecated)
                .map(FallbackKey::getKey)
                .collect(Collectors.toList());
    }

}
