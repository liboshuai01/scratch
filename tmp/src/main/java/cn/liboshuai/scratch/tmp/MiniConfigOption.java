package cn.liboshuai.scratch.tmp;

import lombok.Getter;

import java.util.Objects;

// 1. 泛型类，T 代表配置值的类型
@Getter
public class MiniConfigOption<T> {
    private final String key;
    private final T defaultValue;
    // 2. 核心：运行时保留类型信息 (Type Token)
    private final Class<T> clazz;

    public MiniConfigOption(String key, Class<T> clazz, T defaultValue) {
        this.key = Objects.requireNonNull(key);
        this.clazz = Objects.requireNonNull(clazz);
        this.defaultValue = defaultValue;
    }

}
