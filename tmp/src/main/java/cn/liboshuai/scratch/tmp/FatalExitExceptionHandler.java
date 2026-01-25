package cn.liboshuai.scratch.tmp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 简化版，评估代码差异时，请忽略此类
 */
public final class FatalExitExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FatalExitExceptionHandler.class);

    public static final FatalExitExceptionHandler INSTANCE = new FatalExitExceptionHandler();
    public static final int EXIT_CODE = -17;

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error(
                "FATAL: Thread '{}' produced an uncaught exception. Stopping the process...",
                t.getName(),
                e);
        ThreadUtils.errorLogThreadDump(LOG);
    }
}

