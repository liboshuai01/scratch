package cn.liboshuai.scratch.flink.mini.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface ScheduledExecutor extends Executor {

    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit timeUnit);

    <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit timeUnit);

    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit timeUnit);

    ScheduledFuture<?> scheduledWithFixedDelay (Runnable command, long initialDelay, long delay, TimeUnit timeUnit);
}
