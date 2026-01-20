package cn.liboshuai.scratch.flink.mini.mailbox;


@FunctionalInterface
public interface ThrowingRunnable<E extends Throwable> {
    void run() throws E;
}

