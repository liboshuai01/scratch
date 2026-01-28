package cn.liboshuai.scratch.flink.mini.util.concurrent;

import cn.liboshuai.scratch.flink.mini.util.function.RunnableWithException;
import cn.liboshuai.scratch.flink.mini.util.function.SupplierWithException;
import com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;

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
        return retry(operation, retries, throwable -> true, executor);
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
                                        "Stopped retrying the operation because the error is not retryable.", throwable
                                )
                        );
                    } else {
                        if (retries <= 0) {
                            resultFuture.completeExceptionally(
                                    new RetryException(
                                            "Could not complete the operation. Number of retries "
                                                    + "has been exhausted.", throwable
                                    )
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

    public static <T> CompletableFuture<T> retryWithDelay(
            Supplier<CompletableFuture<T>> operation,
            RetryStrategy retryStrategy,
            ScheduledExecutor scheduledExecutor
    ) {
        return retryWithDelay(operation, retryStrategy, throwable -> true, scheduledExecutor);
    }

    public static <T> CompletableFuture<T> retryWithDelay(
            Supplier<CompletableFuture<T>> operation,
            RetryStrategy retryStrategy,
            Predicate<Throwable> retryPredicate,
            ScheduledExecutor scheduledExecutor
    ) {
        final CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retryOperationWithDelay(resultFuture, operation, retryStrategy, retryPredicate, scheduledExecutor);
        return resultFuture;
    }

    public static <T> void retryOperationWithDelay(
            CompletableFuture<T> resultFuture,
            Supplier<CompletableFuture<T>> operation,
            RetryStrategy retryStrategy,
            Predicate<Throwable> retryPredicate,
            ScheduledExecutor scheduledExecutor
    ) {
        if (resultFuture.isDone()) {
            return;
        }
        CompletableFuture<T> operationFuture = operation.get();
        operationFuture.whenComplete(
                (T value, Throwable throwable) -> {
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
                                                "Stopped retrying the operation because the error is not retryable.", throwable
                                        )
                                );
                            } else {
                                int remainingRetries = retryStrategy.getNumRemainingRetries();
                                if (remainingRetries <= 0) {
                                    resultFuture.completeExceptionally(
                                            new RetryException(
                                                    "Could not complete the operation. Number of retries "
                                                            + "has been exhausted.", throwable
                                            )
                                    );
                                } else {
                                    ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(
                                            () -> retryOperationWithDelay(
                                                    resultFuture,
                                                    operation,
                                                    retryStrategy.getNextRetryStrategy(),
                                                    retryPredicate,
                                                    scheduledExecutor
                                            ),
                                            retryStrategy.getRetryDelay().toMillis(),
                                            TimeUnit.MILLISECONDS
                                    );
                                    resultFuture.whenComplete((T innerValue, Throwable innerThrowable) -> {
                                        scheduledFuture.cancel(false);
                                    });
                                }
                            }
                        }
                    }
                }
        );

        resultFuture.whenComplete((T ignored, Throwable throwable) -> operationFuture.cancel(false));
    }

    public static <T> CompletableFuture<T> retrySuccessfulWithDelay(
            Supplier<CompletableFuture<T>> operation,
            Duration retryDelay,
            Deadline deadline,
            Predicate<T> acceptancePredicate,
            ScheduledExecutor scheduledExecutor
    ) {
        final CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retrySuccessfulOperationWithDelay(resultFuture, operation, retryDelay, deadline, acceptancePredicate, scheduledExecutor);
        return resultFuture;
    }

    public static <T> void retrySuccessfulOperationWithDelay(
            CompletableFuture<T> resultFuture,
            Supplier<CompletableFuture<T>> operation,
            Duration retryDelay,
            Deadline deadline,
            Predicate<T> acceptancePredicate,
            ScheduledExecutor scheduledExecutor
    ) {
        if (resultFuture.isDone()) {
            return;
        }
        CompletableFuture<T> operationFuture = operation.get();
        operationFuture.whenComplete((T value, Throwable throwable) -> {
            if (throwable != null) {
                if (throwable instanceof CancellationException) {
                    resultFuture.completeExceptionally(
                            new RetryException("Operation future was cancelled.", throwable));
                } else {
                    resultFuture.completeExceptionally(throwable);
                }
            } else {
                if (acceptancePredicate.test(value)) {
                    resultFuture.complete(value);
                } else {
                    if (!deadline.hasTimeLeft()) {
                        resultFuture.completeExceptionally(
                                new RetryException(
                                        "Could not satisfy the predicate within the allowed time."));
                    } else {
                        ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(
                                () -> retrySuccessfulOperationWithDelay(
                                        resultFuture,
                                        operation,
                                        retryDelay,
                                        deadline,
                                        acceptancePredicate,
                                        scheduledExecutor
                                ),
                                retryDelay.toMillis(),
                                TimeUnit.MILLISECONDS
                        );
                        resultFuture.whenComplete((T innerValue, Throwable innerThrowable) -> {
                            scheduledFuture.cancel(false);
                        });
                    }
                }
            }
        });

        resultFuture.whenComplete((T ignored, Throwable throwable) -> {
            operationFuture.cancel(false);
        });
    }

    public static ConjunctFuture<Void> waitForAll(Collection<? extends CompletableFuture<?>> futures) {
        Preconditions.checkNotNull(futures, "futures");
        return new WaitingConjunctFuture(futures);
    }

    public static <T> ConjunctFuture<Collection<T>> combineAll(Collection<? extends CompletableFuture<? extends T>> futures) {
        Preconditions.checkNotNull(futures, "futures");
        return new ResultConjunctFuture<>(futures);
    }

    public static ConjunctFuture<Void> completeAll(Collection<? extends CompletableFuture<?>> futures) {
        Preconditions.checkNotNull(futures, "futures");
        return new CompletionConjunctFuture(futures);
    }

    public abstract static class ConjunctFuture<T> extends CompletableFuture<T> {
        abstract int getNumFuturesTotal();

        abstract int getNumFuturesCompleted();
    }

    public static class CompletionConjunctFuture extends ConjunctFuture<Void> {

        private final int numTotal;
        private int numCompleted;
        private Throwable globalThrowable;
        private final Object lock = new Object();

        private void handleCompletedFuture(Object ignored, Throwable throwable) {
            synchronized (lock) {
                numCompleted++;
                if (throwable != null) {
                    globalThrowable = ExceptionUtils.firstOrSuppressed(throwable, globalThrowable);
                }
                if (numTotal == numCompleted) {
                    if (globalThrowable == null) {
                        complete(null);
                    } else {
                        completedExceptionally(globalThrowable);
                    }
                }
            }
        }

        public CompletionConjunctFuture(Collection<? extends CompletableFuture<?>> futures) {
            this.numTotal = futures.size();

            if (futures.isEmpty()) {
                complete(null);
            } else {
                for (CompletableFuture<?> future : futures) {
                    future.whenComplete(this::handleCompletedFuture);
                }
            }
        }

        @Override
        int getNumFuturesTotal() {
            return this.numTotal;
        }

        @Override
        int getNumFuturesCompleted() {
            synchronized (lock) {
                return this.numCompleted;
            }
        }
    }

    public static class ResultConjunctFuture<T> extends ConjunctFuture<Collection<T>> {

        private final int numTotal;
        private final AtomicInteger numCompleted = new AtomicInteger(0);
        private final T[] results;

        private void handleCompletedFuture(int index, T value, Throwable throwable) {
            if (throwable != null) {
                completedExceptionally(throwable);
            } else {
                results[index] = value;
                if (numTotal == numCompleted.incrementAndGet()) {
                    complete(Arrays.asList(results));
                }
            }
        }

        @SuppressWarnings("unchecked")
        public ResultConjunctFuture(Collection<? extends CompletableFuture<? extends T>> futures) {
            this.numTotal = futures.size();
            this.results = (T[]) new Object[numTotal];
            if (futures.isEmpty()) {
                complete(Collections.emptyList());
            } else {
                int counter = 0;
                for (CompletableFuture<? extends T> future : futures) {
                    final int index = counter++;
                    future.whenComplete((T value, Throwable throwable) -> this.handleCompletedFuture(index, value, throwable));
                }
            }
        }

        @Override
        int getNumFuturesTotal() {
            return 0;
        }

        @Override
        int getNumFuturesCompleted() {
            return 0;
        }
    }

    public static class WaitingConjunctFuture extends ConjunctFuture<Void> {

        private final int numTotal;
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        private void handleCompletedFuture(Object ignored, Throwable throwable) {
            if (throwable != null) {
                completedExceptionally(throwable);
            } else {
                if (numTotal == numCompleted.incrementAndGet()) {
                    complete(null);
                }
            }
        }

        public WaitingConjunctFuture(Collection<? extends CompletableFuture<?>> futures) {
            this.numTotal = futures.size();
            if (futures.isEmpty()) {
                complete(null);
            } else {
                for (CompletableFuture<?> future : futures) {
                    future.whenComplete(this::handleCompletedFuture);
                }
            }
        }

        @Override
        int getNumFuturesTotal() {
            return this.numTotal;
        }

        @Override
        int getNumFuturesCompleted() {
            return this.numCompleted.get();
        }
    }

    public static <IN, OUT> CompletableFuture<OUT> thenApplyAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            Function<? super IN, ? extends OUT> applyFun
    ) {
        return completableFuture.isDone()
                ? completableFuture.thenApply(applyFun)
                : completableFuture.thenApplyAsync(applyFun, executor);
    }

    public static <IN, OUT> CompletableFuture<OUT> thenComposeAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            Function<? super IN, ? extends CompletionStage<OUT>> applyFun
    ){
        return completableFuture.isDone()
                ? completableFuture.thenCompose(applyFun)
                : completableFuture.thenComposeAsync(applyFun, executor);
    }

    public static <IN> CompletableFuture<IN> whenCompleteAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            BiConsumer<? super IN, ? super Throwable> biConsumer
    ) {
        return completableFuture.isDone()
                ? completableFuture.whenComplete(biConsumer)
                : completableFuture.whenCompleteAsync(biConsumer, executor);
    }

    public static <IN> CompletableFuture<Void> thenAcceptAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            Consumer<IN> consumer
    ) {
        return completableFuture.isDone()
                ? completableFuture.thenAccept(consumer)
                : completableFuture.thenAcceptAsync(consumer, executor);
    }

    public static <IN, OUT> CompletableFuture<OUT> handleAsyncIfNotDone(
            CompletableFuture<IN> completableFuture,
            Executor executor,
            BiFunction<? super IN, Throwable, ? extends OUT> biFunction
    ) {
        return completableFuture.isDone()
                ? completableFuture.handle(biFunction)
                : completableFuture.handleAsync(biFunction, executor);
    }

    private static class Timeout implements Runnable {

        private final CompletableFuture<?> future;
        private final String timeoutMsg;

        private Timeout(CompletableFuture<?> future, String timeoutMsg) {
            this.future = future;
            this.timeoutMsg = timeoutMsg;
        }

        @Override
        public void run() {
            future.completeExceptionally(new TimeoutException(timeoutMsg));
        }
    }

    private enum Delayer {
        ;
        private static final ScheduledExecutorService DELAYER =
                new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("FlinkCompletableFutureDelayScheduler"));

        public static ScheduledFuture<?> delay(Runnable command, long delay, TimeUnit timeUnit) {
            return DELAYER.schedule(command, delay, timeUnit);
        }
    }

    /**
     * 与Flink中的代码进行对比时，请忽略这个 ExecutorThreadFactory 的建构区别
     */
    private static class ExecutorThreadFactory implements ThreadFactory {

        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(0);

        private ExecutorThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(@NonNull Runnable r) {
            Thread thread = new Thread(r, prefix + counter.getAndIncrement());
            thread.setDaemon(true);
            thread.setPriority(Thread.NORM_PRIORITY);
            return thread;
        }
    }

    public static <T> CompletableFuture<T> orTimeout(
            CompletableFuture<T> completableFuture,
            long timeout,
            TimeUnit timeUnit,
            Executor timeoutFailExecutor,
            @Nullable String timeoutMsg
    ) {
        if (completableFuture.isDone()) {
            return completableFuture;
        }
        ScheduledFuture<?> timeoutFuture = Delayer.delay(
                () -> timeoutFailExecutor.execute(new Timeout(completableFuture, timeoutMsg)),
                timeout,
                timeUnit
        );
        completableFuture.whenComplete((T ignored, Throwable throwable) -> {
            if (!timeoutFuture.isDone()) {
                timeoutFuture.cancel(false);
            }
        });
        return completableFuture;
    }

    public static <T> void completeDelayed(CompletableFuture<T> future, T success, Duration delay) {
        Delayer.delay(() -> future.complete(success), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static CompletableFuture<Void> runAfterwards(
            CompletableFuture<?> future,
            RunnableWithException runnable
    ) {
        return runAfterwardsAsync(future, runnable, Executors.directExecutor());
    }

    public static CompletableFuture<Void> runAfterwardsAsync(
            CompletableFuture<?> future,
            RunnableWithException runnable
    ) {
        return runAfterwardsAsync(future, runnable, ForkJoinPool.commonPool());
    }


    public static CompletableFuture<Void> runAfterwardsAsync(
            CompletableFuture<?> future,
            RunnableWithException runnable,
            Executor executor
    ) {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        future.whenCompleteAsync((Object ignored, Throwable throwable) -> {
            try {
                runnable.run();
            } catch (Throwable e) {
                throwable = ExceptionUtils.firstOrSuppressed(e, throwable);
            }
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
            } else {
                resultFuture.complete(null);
            }
        }, executor);
        return resultFuture;
    }

    public static CompletableFuture<Void> composeAfterwards(
            CompletableFuture<?> future,
            Supplier<CompletableFuture<?>> composedAction
    ) {
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        future.whenComplete((Object outerIgnored, Throwable outerThrowable) -> {
            CompletableFuture<?> composedActionFuture = composedAction.get();
            composedActionFuture.whenComplete((Object innerIgnored, Throwable innerThrowable) -> {
                if (innerThrowable != null) {
                     resultFuture.completeExceptionally(
                             ExceptionUtils.firstOrSuppressed(innerThrowable, outerThrowable)
                     );
                } else if (outerThrowable != null) {
                    resultFuture.completeExceptionally(outerThrowable);
                } else {
                    resultFuture.complete(null);
                }
            });
        });
        return resultFuture;
    }

}
