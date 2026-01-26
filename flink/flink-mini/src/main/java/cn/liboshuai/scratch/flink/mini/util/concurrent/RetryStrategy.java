package cn.liboshuai.scratch.flink.mini.util.concurrent;

import java.time.Duration;

public interface RetryStrategy {

    int getNumRemainingRetries();

    Duration getRetryDelay();

    RetryStrategy getNextRetryStrategy();
}
