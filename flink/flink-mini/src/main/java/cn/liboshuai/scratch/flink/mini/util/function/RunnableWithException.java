package cn.liboshuai.scratch.flink.mini.util.function;

@FunctionalInterface
public interface RunnableWithException extends ThrowingRunnable<Exception> {
    @Override
    void run() throws Exception;
}
