package cn.liboshuai.scratch.flink.mini;


@FunctionalInterface
public interface ThrowingRunnable<E extends Throwable> {
    void run() throws E;
}

