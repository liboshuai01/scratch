package cn.liboshuai.scratch.flink.mini.configuration;

public interface WritableConfig {
    <T> WritableConfig set(ConfigOption<T> option, T value);
}
