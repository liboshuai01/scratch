package cn.liboshuai.scratch.tmp;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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
        public ResultConjunctFuture(Collection<? extends CompletableFuture<T>> futures) {
            this.numTotal = futures.size();
            results = (T[]) new Object[numTotal];
            if (futures.isEmpty()) {
                complete(Collections.EMPTY_LIST);
            } else {
                int count = 0;
                for (CompletableFuture<T> future : futures) {
                    int index = count++;
                    future.whenComplete((T value, Throwable throwable) -> this.handleCompletedFuture(index, value, throwable));
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
}
