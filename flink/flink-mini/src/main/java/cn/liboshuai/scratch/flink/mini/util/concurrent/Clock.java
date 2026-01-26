package cn.liboshuai.scratch.flink.mini.util.concurrent;

public abstract class Clock {

    public abstract long absoluteTimeMillis();

    public abstract long relativeTimeMillis();

    public abstract long relativeTimeNanos();

}
