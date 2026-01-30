package cn.liboshuai.scratch.flink.mini.configuration;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConfigOptions {

    private ConfigOptions() {

    }

    @SuppressWarnings("unchecked")
    private static final Class<Map<String, String>> PROPERTIES_MAP_CLASS =
            (Class<Map<String, String>>) (Class<?>) Map.class;

    public static OptionBuilder key(String key) {
        Preconditions.checkNotNull(key);
        return new OptionBuilder(key);
    }

    public static class OptionBuilder {
        private final String key;

        public OptionBuilder(String key) {
            this.key = key;
        }

        public TypeConfigOptionBuilder<String> stringType() {
            return new TypeConfigOptionBuilder<>(key, String.class);
        }

        public TypeConfigOptionBuilder<Integer> intType() {
            return new TypeConfigOptionBuilder<>(key, Integer.class);
        }

        public TypeConfigOptionBuilder<Long> longType() {
            return new TypeConfigOptionBuilder<>(key, Long.class);
        }

        public TypeConfigOptionBuilder<Boolean> booleanType() {
            return new TypeConfigOptionBuilder<>(key, Boolean.class);
        }

        public TypeConfigOptionBuilder<Float> floatType() {
            return new TypeConfigOptionBuilder<>(key, Float.class);
        }

        public TypeConfigOptionBuilder<Double> doubleType() {
            return new TypeConfigOptionBuilder<>(key, Double.class);
        }

        public TypeConfigOptionBuilder<Duration> durationType() {
            return new TypeConfigOptionBuilder<>(key, Duration.class);
        }

        public <T extends Enum<T>> TypeConfigOptionBuilder<T> enumType(Class<T> enumClass) {
            return new TypeConfigOptionBuilder<>(key, enumClass);
        }

        public TypeConfigOptionBuilder<Map<String, String>> mapType() {
            return new TypeConfigOptionBuilder<>(key, PROPERTIES_MAP_CLASS);
        }

        @Deprecated
        public <T> ConfigOption<T> defaultValue(T value) {
            Preconditions.checkNotNull(value);
            return new ConfigOption<>(key, value, null, value.getClass(), false);
        }

        @Deprecated
        public <T> ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(key, null, null, String.class, false);
        }

    }

    public static class TypeConfigOptionBuilder<T> {
        private final String key;
        private final Class<T> clazz;

        TypeConfigOptionBuilder(String key, Class<T> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        public ConfigOption<T> defaultValue(T value) {
            Preconditions.checkNotNull(value);
            return new ConfigOption<>(key, value, null, clazz, false);
        }

        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(key, null, null, clazz, false);
        }
    }

    public static class ListConfigOptionBuilder<E> {
        private final String key;
        private final Class<E> clazz;

        public ListConfigOptionBuilder(String key, Class<E> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        @SafeVarargs
        public final ConfigOption<List<E>> defaultValue(E... values) {
            return new ConfigOption<>(key, Arrays.asList(values), null, clazz, true);
        }

        public ConfigOption<List<E>> noDefaultValue() {
            return new ConfigOption<>(key, null, null, clazz, true);
        }
    }
}
