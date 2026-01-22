package cn.liboshuai.scratch.tmp;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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
}
