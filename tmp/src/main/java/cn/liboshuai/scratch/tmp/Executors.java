package cn.liboshuai.scratch.tmp;

import java.util.concurrent.Executor;

public class Executors {

    private Executors() {
    }

    public static Executor directExecutor() {
        return DirectExecutor.INSTANCE;
    }

    private enum DirectExecutor implements Executor {

        INSTANCE;

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
