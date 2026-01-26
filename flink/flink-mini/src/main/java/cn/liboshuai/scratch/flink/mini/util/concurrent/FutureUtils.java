package cn.liboshuai.scratch.flink.mini.util.concurrent;

import cn.liboshuai.scratch.flink.mini.util.function.SupplierWithException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;

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

    public static <T> CompletableFuture<T> supplyAsync(
            SupplierWithException<T, ?> supplier,
            Executor executor
    ) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return supplier.get();
                    } catch (Throwable throwable) {
                        throw new CompletionException(throwable);
                    }
                },
                executor
        );
    }

    public static <T> CompletableFuture<T> retry(
            Supplier<CompletableFuture<T>> operation,
            int retries,
            Executor executor
    ) {
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retryOperation(resultFuture, operation, retries, throwable -> true, executor);
        return resultFuture;
    }

    public static <T> CompletableFuture<T> retry(
            Supplier<CompletableFuture<T>> operation,
            int retries,
            Predicate<Throwable> retryPredicate,
            Executor executor
    ) {
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retryOperation(resultFuture, operation, retries, retryPredicate, executor);
        return resultFuture;
    }

    public static <T> void retryOperation(
            CompletableFuture<T> resultFuture,
            Supplier<CompletableFuture<T>> operation,
            int retries,
            Predicate<Throwable> retryPredicate,
            Executor executor
    ) {
        if (resultFuture.isDone()) {
            return;
        }
        CompletableFuture<? extends T> operationFuture = operation.get();
        operationFuture.whenCompleteAsync((T value, Throwable throwable) -> {
            if (throwable == null) {
                resultFuture.complete(value);
            } else {
                if (throwable instanceof CancellationException) {
                    resultFuture.completeExceptionally(
                            new RetryException("Operation future was cancelled.", throwable)
                    );
                } else {
                    throwable = ExceptionUtils.stripCompletionException(throwable);
                    if (!retryPredicate.test(throwable)) {
                        resultFuture.completeExceptionally(
                                new RetryException(
                                        "Stopped retrying the operation because the error is not "
                                                + "retryable.",
                                        throwable)
                        );
                    } else {
                        if (retries <= 0) {
                            resultFuture.completeExceptionally(
                                    new RetryException(
                                            "Could not complete the operation. Number of retries "
                                                    + "has been exhausted.",
                                            throwable)
                            );
                        } else {
                            retryOperation(
                                    resultFuture,
                                    operation,
                                    retries - 1,
                                    retryPredicate,
                                    executor
                            );
                        }
                    }
                }
            }
        }, executor);
        resultFuture.whenComplete((T ignored, Throwable throwable) -> {
            operationFuture.cancel(false);
        });
    }

    public static class RetryException extends Exception {

        private static final long serialVersionUID = 4505379678904970084L;

        public RetryException(String message) {
            super(message);
        }

        public RetryException(String message, Throwable throwable) {
            super(message, throwable);
        }

        public RetryException(Throwable throwable) {
            super(throwable);
        }
    }

}
