package cn.liboshuai.scratch.flink.mini.configuration;

import java.util.Optional;

public interface ReadableConfig {

    <T> T get(ConfigOption<T> option);

    <T> Optional<T> getOptional(ConfigOption<T> option);
}
