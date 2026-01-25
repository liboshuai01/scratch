package cn.liboshuai.scratch.flink.mini.util.concurrent;

import java.util.concurrent.CompletableFuture;

/**
 * 仿写Flink中的FutureUtils类，依次来提高自己Java异步编程和函数式编程的能力
 */
public class FutureUtils {

    private static final CompletableFuture<Void> COMPLETED_VOID_FUTURE = CompletableFuture.completedFuture(null);

    public static CompletableFuture<Void> completedVoidFuture() {
        return COMPLETED_VOID_FUTURE;
    }

}
