package cn.liboshuai.scratch.flink.mini.util.concurrent;

import java.util.concurrent.CompletableFuture;

/**
 * 仿写Flink中的FutureUtils类，依次来提高自己Java异步编程和函数式编程的能力
 */
public class FutureUtils {

    private static final CompletableFuture<Void> COMPLETED_VOID_FUTURE = CompletableFuture.completedFuture(null);

    private static final CompletableFuture<?> UNSUPPORTED_OPERATION_FUTURE = completedExceptionally(
            new UnsupportedOperationException("The method is unsupported.")
    );

    public static CompletableFuture<Void> completedVoidFuture() {
        return COMPLETED_VOID_FUTURE;
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> unsupportedOperationFuture() {
        return (CompletableFuture<T>) UNSUPPORTED_OPERATION_FUTURE;
    }

    public static <T> CompletableFuture<T> completedExceptionally(Throwable throwable) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(throwable);
        return completableFuture;
    }

}
