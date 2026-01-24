package cn.liboshuai.scratch.tmp;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface ScheduledExecutor extends Executor {

    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit timeUnit);

    <T> ScheduledFuture<T> schedule(Callable<T> callable, long delay, TimeUnit timeUnit);

    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, int period, TimeUnit timeUnit);

    ScheduledFuture<?> scheduleWithFixedRate(Runnable command, long initialDelay, int delay, TimeUnit timeUnit);
}
