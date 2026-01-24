package cn.liboshuai.scratch.tmp;

import com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorServiceAdapter implements ScheduledExecutor {

    private final ScheduledExecutorService scheduledExecutorService;

    public ScheduledExecutorServiceAdapter(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = Preconditions.checkNotNull(scheduledExecutorService);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit timeUnit) {
        return scheduledExecutorService.schedule(command, delay, timeUnit);
    }

    @Override
    public <T> ScheduledFuture<T> schedule(Callable<T> callable, long delay, TimeUnit timeUnit) {
        return scheduledExecutorService.schedule(callable, delay, timeUnit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, int period, TimeUnit timeUnit) {
        return scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, timeUnit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedRate(Runnable command, long initialDelay, int delay, TimeUnit timeUnit) {
        return scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, timeUnit);
    }

    @Override
    public void execute(@NonNull Runnable command) {
        scheduledExecutorService.execute(command);
    }
}
