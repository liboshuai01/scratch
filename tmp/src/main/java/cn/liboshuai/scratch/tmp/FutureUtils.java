package cn.liboshuai.scratch.tmp;

import com.google.common.base.Supplier;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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
                            resultFuture.completeExceptionally(new RetryException("Stopped retrying the operation because the error is not " + "retryable.", throwable));
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

}
