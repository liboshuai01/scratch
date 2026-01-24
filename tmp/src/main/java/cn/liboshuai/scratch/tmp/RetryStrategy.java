package cn.liboshuai.scratch.tmp;

import java.time.Duration;

public interface RetryStrategy {
    long getNumRemainingRetries();
    Duration getCurrentRetryDelay();
    RetryStrategy getNextRetryStrategy();
}
