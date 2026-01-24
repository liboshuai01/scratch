package cn.liboshuai.scratch.tmp;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 模仿Flink中的FutureUtils，以此来提升自己对于异步编程和函数式编程的能力
 */
public class FutureUtils {

    public abstract static class ConjunctFuture<T> extends CompletableFuture<T> {
        public abstract int getNumFuturesTotal();

        public abstract int getNumFuturesCompleted();
    }

    public static final class WaitingConjunctFuture extends ConjunctFuture<Void> {

        private final int numTotal;
        private final AtomicInteger numCompleted = new AtomicInteger(0);

        public void handleCompletedFuture(Object ignored, Throwable throwable) {
            if (throwable != null) {
                completeExceptionally(throwable);
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
        public int getNumFuturesTotal() {
            return this.numTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            return this.numCompleted.get();
        }
    }

    public static final class ResultConjunctFuture<T> extends ConjunctFuture<Collection<T>> {

        private final int numTotal;
        private final AtomicInteger numCompleted = new AtomicInteger(0);
        private final T[] results;

        public void handleCompletedFuture(int index, T value, Throwable throwable) {
            if (throwable == null) {
                results[index] = value;
                if (numTotal == numCompleted.incrementAndGet()) {
                    complete(Arrays.asList(results));
                }
            } else {
                completeExceptionally(throwable);
            }
        }

        @SuppressWarnings("unchecked")
        public ResultConjunctFuture(Collection<? extends CompletableFuture<? extends T>> futures) {
            this.numTotal = futures.size();
            results = (T[]) new Object[numTotal];
            if (futures.isEmpty()) {
                complete(Collections.EMPTY_LIST);
            } else {
                int count = 0;
                for (CompletableFuture<? extends T> future : futures) {
                    int index = count++;
                    future.whenComplete((T value, Throwable throwable) -> this.handleCompletedFuture(index, value, throwable));
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return this.numTotal;
        }

        @Override
        public int getNumFuturesCompleted() {
            return this.numCompleted.get();
        }
    }

    public static final class CompletedConjunctFuture extends ConjunctFuture<Void> {

        private final Object lock = new Object();

        private final int numTotal;
        private int futuresCompleted;
        private Throwable globalThrowable;

        public CompletedConjunctFuture(Collection<? extends CompletableFuture<?>> futures) {
            this.numTotal = futures.size();
            if (futures.isEmpty()) {
                complete(null);
            } else {
                for (CompletableFuture<?> future : futures) {
                    future.whenComplete(this::handleCompleted);
                }
            }
        }

        private void handleCompleted(Object ignored, Throwable throwable) {
            synchronized (lock) {
                futuresCompleted++;
                if (throwable != null) {
                    globalThrowable = ExceptionUtils.firstOrSuppressed(throwable, globalThrowable);
                }
                if (numTotal == futuresCompleted) {
                    if (globalThrowable == null) {
                        complete(null);
                    } else {
                        completeExceptionally(globalThrowable);
                    }
                }
            }
        }

        @Override
        public int getNumFuturesTotal() {
            return 0;
        }

        @Override
        public int getNumFuturesCompleted() {
            return 0;
        }
    }

    public static ConjunctFuture<Void> waitForAll(Collection<? extends CompletableFuture<?>> futures) {
        checkNotNull(futures, "futures");
        return new WaitingConjunctFuture(futures);
    }

    public static <T> ConjunctFuture<Collection<T>> combineForAll(Collection<? extends CompletableFuture<T>> futures) {
        checkNotNull(futures, "futures");
        return new ResultConjunctFuture<>(futures);
    }

    public static ConjunctFuture<Void> completedForAll(Collection<? extends CompletableFuture<?>> futures) {
        checkNotNull(futures, "futures");
        return new CompletedConjunctFuture(futures);
    }

    public static class RetryException extends Exception {

        private static final long serialVersionUID = 6075310037996807663L;

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

    public static <T> CompletableFuture<T> retry(Supplier<CompletableFuture<T>> operation, int retries, Executor executor) {
        return retry(operation, retries, ignore -> true, executor);
    }

    public static <T> CompletableFuture<T> retry(Supplier<CompletableFuture<T>> operation, int retries, Predicate<Throwable> retryPredicate, Executor executor) {
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retryOperation(resultFuture, operation, retries, retryPredicate, executor);
        return resultFuture;
    }

    public static <T> void retryOperation(CompletableFuture<T> resultFuture, Supplier<CompletableFuture<T>> operation, int retries, Predicate<Throwable> retryPredicate, Executor executor) {
        if (resultFuture.isDone()) {
            return;
        }
        CompletableFuture<T> operationFuture = operation.get();
        operationFuture.whenCompleteAsync((T value, Throwable throwable) -> {
            if (throwable != null) {
                if (throwable instanceof CancellationException) {
                    resultFuture.completeExceptionally(new RetryException("Operation future was cancelled.", throwable));
                } else {
                    throwable = ExceptionUtils.stripExecutionException(throwable);
                    if (retryPredicate.test(throwable)) {
                        if (retries > 0) {
                            retryOperation(resultFuture, operation, retries - 1, retryPredicate, executor);
                        } else {
                            resultFuture.completeExceptionally(new RetryException("Could not complete the operation. Number of retries has been exhausted.", throwable));
                        }
                    } else {
                        resultFuture.completeExceptionally(new RetryException("Stopped retrying the operation because the error is not " + "retryable.", throwable));
                    }
                }
            } else {
                resultFuture.complete(value);
            }
        }, executor);
        resultFuture.whenComplete((T ignore, Throwable throwable) -> operationFuture.cancel(false));
    }

    public static <T> CompletableFuture<T> retryWithDelay(
            Supplier<CompletableFuture<T>> operation,
            RetryStrategy retryStrategy,
            ScheduledExecutor scheduledExecutor
    ) {
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retryOperationWithDelay(resultFuture, operation, retryStrategy, ignore -> true, scheduledExecutor);
        return resultFuture;
    }

    public static <T> CompletableFuture<T> retryWithDelay(
            Supplier<CompletableFuture<T>> operation,
            RetryStrategy retryStrategy,
            Predicate<Throwable> retryPredicate,
            ScheduledExecutor scheduledExecutor
    ) {
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retryOperationWithDelay(resultFuture, operation, retryStrategy, retryPredicate, scheduledExecutor);
        return resultFuture;
    }

    private static <T> void retryOperationWithDelay(CompletableFuture<T> resultFuture, Supplier<CompletableFuture<T>> operation, RetryStrategy retryStrategy, Predicate<Throwable> retryPredicate, ScheduledExecutor scheduledExecutor) {
        if (resultFuture.isDone()) {
            return;
        }
        CompletableFuture<T> operationFuture = operation.get();
        operationFuture.whenComplete((T value, Throwable throwable) -> {
            if (throwable != null) {
                if (throwable instanceof CancellationException) {
                    resultFuture.completeExceptionally(new RetryException("Operation future was cancelled.", throwable));
                } else {
                    throwable = ExceptionUtils.stripExecutionException(throwable);
                    if (retryPredicate.test(throwable)) {
                        long numRemainingRetries = retryStrategy.getNumRemainingRetries();
                        if (numRemainingRetries > 0) {
                            Duration currentRetryDelay = retryStrategy.getCurrentRetryDelay();
                            ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(
                                    () -> retryOperationWithDelay(
                                            resultFuture,
                                            operation,
                                            retryStrategy.getNextRetryStrategy(),
                                            retryPredicate,
                                            scheduledExecutor
                                    ),
                                    currentRetryDelay.toMillis(),
                                    TimeUnit.MILLISECONDS
                            );
                            resultFuture.whenComplete((T ignore, Throwable wthrowable) -> scheduledFuture.cancel(false));
                        } else {
                            resultFuture.completeExceptionally(new RetryException("Could not complete the operation. Number of retries has been exhausted.", throwable));
                        }
                    } else {
                        resultFuture.completeExceptionally(new RetryException("Stopped retrying the operation because the error is not " + "retryable.", throwable));
                    }
                }
            } else {
                resultFuture.complete(value);
            }
        });

        resultFuture.whenComplete((T ignore, Throwable wthrowable) -> operationFuture.cancel(false));
    }

    public static <T> CompletableFuture<T> retrySuccessfulWithDelay(
            final Supplier<CompletableFuture<T>> operation,
            final Duration retryDelay,
            final Deadline deadline,
            final Predicate<T> acceptancePredicate,
            final ScheduledExecutor scheduledExecutor
    ) {
        final CompletableFuture<T> resultFuture = new CompletableFuture<>();
        retrySuccessfulOperationWithDelay(resultFuture, operation, retryDelay, deadline, acceptancePredicate, scheduledExecutor);
        return resultFuture;
    }

    private static <T> void retrySuccessfulOperationWithDelay(CompletableFuture<T> resultFuture, Supplier<CompletableFuture<T>> operation, Duration retryDelay, Deadline deadline, Predicate<T> acceptancePredicate, ScheduledExecutor scheduledExecutor) {
        if (resultFuture.isDone()) {
            return;
        }
        CompletableFuture<T> operationFuture = operation.get();
        operationFuture.whenComplete((T value, Throwable throwable) -> {
            if (throwable != null) {
                if (throwable instanceof CancellationException) {
                    resultFuture.completeExceptionally(new RetryException("Operation future was cancelled.", throwable));
                } else {
                    resultFuture.completeExceptionally(throwable);
                }
            } else {
                if (!acceptancePredicate.test(value)) {
                    if (deadline.hasTimeLeft()) {
                        final ScheduledFuture<?> scheduledFuture = scheduledExecutor.schedule(
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
                        resultFuture.whenComplete((T ignore, Throwable t) -> scheduledFuture.cancel(false));
                    } else {
                        resultFuture.completeExceptionally(
                                new RetryException(
                                        "Could not satisfy the predicate within the allowed time."));
                    }
                } else {
                    resultFuture.complete(value);
                }
            }
        });
        resultFuture.whenComplete((T ignore, Throwable throwable) -> operationFuture.cancel(false));
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

}
