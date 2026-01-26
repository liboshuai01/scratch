package cn.liboshuai.scratch.flink.mini.util.concurrent;

import com.google.common.base.Preconditions;

import java.time.Duration;

public final class ExponentailBackoffRetryStrategy implements RetryStrategy {

    private final int remainReties;
    private final Duration currentRetryDelay;
    private final Duration maxRetryDelay;

    public ExponentailBackoffRetryStrategy(int remainReties, Duration currentRetryDelay, Duration maxRetryDelay) {
        Preconditions.checkArgument(remainReties >= 0, "The number of retries must be greater or equal to 0.");
        Preconditions.checkArgument(currentRetryDelay.toMillis() >= 0, "The currentRetryDelay must be positive");
        Preconditions.checkArgument(maxRetryDelay.toMillis() >= 0, "The maxRetryDelay must be positive");
        this.remainReties = remainReties;
        this.currentRetryDelay = currentRetryDelay;
        this.maxRetryDelay = maxRetryDelay;
    }

    @Override
    public int getNumRemainingRetries() {
        return this.remainReties;
    }

    @Override
    public Duration getRetryDelay() {
        return this.currentRetryDelay;
    }

    @Override
    public RetryStrategy getNextRetryStrategy() {
        int nextRemainReties = remainReties - 1;
        long nextRetryDelayMillis = Math.min(currentRetryDelay.toMillis() * 2, maxRetryDelay.toMillis());
        return new ExponentailBackoffRetryStrategy(
                nextRemainReties,
                Duration.ofMillis(nextRetryDelayMillis),
                maxRetryDelay
        );
    }
}
