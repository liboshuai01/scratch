package cn.liboshuai.scratch.tmp;

import java.time.Duration;

public interface RetryStrategy {
    int getNumRemainingRetries();
    Duration getCurrentRetryDelay();
    RetryStrategy getNextRetryStrategy();
}
