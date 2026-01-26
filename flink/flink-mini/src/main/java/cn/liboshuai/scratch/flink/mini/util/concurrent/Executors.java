package cn.liboshuai.scratch.flink.mini.util.concurrent;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.Executor;

public class Executors {

    private Executors() {

    }

    public static Executor directExecutor() {
        return DirectorExecutor.INSTANCE;
    }


    private enum DirectorExecutor implements Executor {
        INSTANCE;

        @Override
        public void execute(@NonNull Runnable command) {
            command.run();
        }
    }
}
