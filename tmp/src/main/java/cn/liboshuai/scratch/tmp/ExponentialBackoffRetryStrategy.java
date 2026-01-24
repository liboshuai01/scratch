package cn.liboshuai.scratch.tmp;

import com.google.common.base.Preconditions;

import java.time.Duration;


public class ExponentialBackoffRetryStrategy implements RetryStrategy {

    private final long remainingRetries;
    private final Duration currentRetryDelay;
    private final Duration maxRetryDelay;

    public ExponentialBackoffRetryStrategy(long remainingRetries, Duration currentRetryDelay, Duration maxRetryDelay) {
        Preconditions.checkArgument(remainingRetries >= 0, "The number of retries must be greater or equal to 0.");
        Preconditions.checkArgument(currentRetryDelay.toMillis() >= 0, "The currentRetryDelay must be positive");
        Preconditions.checkArgument(maxRetryDelay.toMillis() >= 0, "The maxRetryDelay must be positive");
        this.remainingRetries = remainingRetries;
        this.currentRetryDelay = currentRetryDelay;
        this.maxRetryDelay = maxRetryDelay;
    }

    @Override
    public long getNumRemainingRetries() {
        return this.remainingRetries;
    }

    @Override
    public Duration getCurrentRetryDelay() {
        return this.currentRetryDelay;
    }

    @Override
    public RetryStrategy getNextRetryStrategy() {
        long nextRemainRetries = remainingRetries - 1;
        long nextRetryDelayMillis = Math.min(2 * currentRetryDelay.toMillis(), maxRetryDelay.toMillis());
        return new ExponentialBackoffRetryStrategy(
                nextRemainRetries,
                Duration.ofMillis(nextRetryDelayMillis),
                maxRetryDelay
        );
    }
}
