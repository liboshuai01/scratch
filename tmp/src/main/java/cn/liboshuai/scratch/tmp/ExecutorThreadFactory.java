package cn.liboshuai.scratch.tmp;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.ThreadFactory;

/**
 * 这里我简化了，没有使用Flink的ExecutorThreadFactory，而是自己来实现了。
 * 评估代码差异的时候，请忽略此类与Flink原生实现的差异。
 */
public class ExecutorThreadFactory implements ThreadFactory {

    private final String pollName;

    public ExecutorThreadFactory(String pollName) {
        this.pollName = pollName;
    }

    @Override
    public Thread newThread(@NonNull Runnable command) {
        Thread thread = new Thread(command, pollName);
        if (!thread.isDaemon()) {
            thread.setDaemon(true);
        }
        thread.setPriority(Thread.NORM_PRIORITY);
        return thread;
    }
}
