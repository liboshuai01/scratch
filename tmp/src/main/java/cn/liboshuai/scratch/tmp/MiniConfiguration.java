package cn.liboshuai.scratch.tmp;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MiniConfiguration {

    // 1. 内部存储：Key 是 String，Value 是 Object (类型擦除的重灾区)
    private final Map<String, Object> confData = new HashMap<>();

    /**
     * 类型安全的 Set 方法
     */
    public <T> void set(MiniConfigOption<T> option, T value) {
        // 2. 线程安全：使用 synchronized (this.confData) 锁住内部状态
        synchronized (confData) {
            confData.put(option.getKey(), value);
        }
    }

    /**
     * 类型安全的 Get 方法
     */
    public <T> T get(MiniConfigOption<T> option) {
        // 3. 复用 getOptional 逻辑，保持代码 DRY (Don't Repeat Yourself)
        return getOptional(option).orElse(option.getDefaultValue());
    }

    /**
     * 核心：利用 Optional 和 Class 对象进行安全转换
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getOptional(MiniConfigOption<T> option) {
        synchronized (confData) {
            Object rawValue = confData.get(option.getKey());

            if (rawValue == null) {
                return Optional.empty();
            }

            // 4. 运行时类型检查与转换 (The Magic Happens Here)
            Class<T> targetClass = option.getClazz();

            // 情况 A: 类型完全匹配 (例如存的是 Integer, 取的也是 Integer)
            if (targetClass.isAssignableFrom(rawValue.getClass())) {
                return Optional.of((T) rawValue);
            }

            // 情况 B: 简单转换 (例如存的是 String "123", 取的是 Integer)
            // * Flink 源码中这部分逻辑在 ConfigurationUtils 里，这里简化实现 *
            if (targetClass == Integer.class && rawValue instanceof String) {
                try {
                    return Optional.of((T) Integer.valueOf((String) rawValue));
                } catch (NumberFormatException e) {
                    // 忽略错误，视为空值或抛出异常均可
                }
            }

            // 更多类型转换逻辑省略...

            throw new IllegalArgumentException(
                    "类型不匹配！Key: " + option.getKey() +
                            " 实际类型: " + rawValue.getClass().getSimpleName() +
                            " 期望类型: " + targetClass.getSimpleName()
            );
        }
    }

    public static void main(String[] args) {
        // 1. 定义配置项 (通常在 ConfigOptions 工厂类中)
        MiniConfigOption<Integer> PORT = new MiniConfigOption<>("rest.port", Integer.class, 8081);
        MiniConfigOption<String> HOST = new MiniConfigOption<>("rest.host", String.class, "localhost");

        // 2. 创建配置容器
        MiniConfiguration conf = new MiniConfiguration();

        // 3. 写入配置
        conf.set(PORT, 9090); // 强类型检查，你无法 set(PORT, "BadValue")

        // 4. 读取配置
        int port = conf.get(PORT); // 自动拆箱，不需要 (int) 强转
        String host = conf.get(HOST); // 使用默认值

        System.out.println("Port: " + port); // 输出 9090
        System.out.println("Host: " + host); // 输出 localhost

        // 5. 模拟从配置文件读取字符串 (动态类型转换)
        // 假设有人手动在 Map 里塞了个 String
        MiniConfiguration fileConf = new MiniConfiguration();
        // 这是一个 hack 操作，模拟解析 yaml 文件后的状态
        // 实际上我们需要暴露一个 setString(key, val) 方法
        // 这里为了演示 MiniConfiguration 的鲁棒性
        // ... (此处略，因为 set 方法限制了类型，说明我们的封装是安全的)
    }
}
