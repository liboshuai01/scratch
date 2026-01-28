package cn.liboshuai.scratch.flink.mini.util.function;

@FunctionalInterface
public interface ThrowingRunnable<E extends Throwable> {
    void run() throws E;
}
