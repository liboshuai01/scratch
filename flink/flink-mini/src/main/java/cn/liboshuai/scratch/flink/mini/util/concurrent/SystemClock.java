package cn.liboshuai.scratch.flink.mini.util.concurrent;

public class SystemClock extends Clock {

    private static final SystemClock INSTANCE = new SystemClock();

    public static SystemClock getInstance() {
        return INSTANCE;
    }

    private SystemClock() {

    }

    @Override
    public long absoluteTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public long relativeTimeMillis() {
        return System.nanoTime() / 1_000_000;
    }

    @Override
    public long relativeTimeNanos() {
        return System.nanoTime();
    }
}
