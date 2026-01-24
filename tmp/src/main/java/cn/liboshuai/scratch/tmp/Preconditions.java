package cn.liboshuai.scratch.tmp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Preconditions {

    public static void checkCompletedNormally(CompletableFuture<?> future) {
        checkState(future.isDone());
        if (future.isCompletedExceptionally()) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static void checkState(boolean condition) {
        if (!condition) {
            throw new IllegalStateException();
        }
    }
}
