package cn.liboshuai.scratch.flink.mini.util.concurrent;

import com.google.common.base.Preconditions;

import java.time.Duration;

public final class FixedRetryStrategy implements RetryStrategy {

    private final int remainReties;
    private final Duration retryDelay;

    public FixedRetryStrategy(int remainReties, Duration retryDelay) {
        Preconditions.checkArgument(remainReties >= 0, "The number of retries must be greater or equal to 0.");
        Preconditions.checkArgument(retryDelay.toMillis() >= 0, "The retryDelay must be positive");
        this.remainReties = remainReties;
        this.retryDelay = retryDelay;
    }

    @Override
    public int getNumRemainingRetries() {
        return this.remainReties;
    }

    @Override
    public Duration getRetryDelay() {
        return retryDelay;
    }

    @Override
    public RetryStrategy getNextRetryStrategy() {
        int nextRemainReties = remainReties - 1;
        return new FixedRetryStrategy(
                nextRemainReties,
                retryDelay
        );
    }
}
