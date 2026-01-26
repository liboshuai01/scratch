package cn.liboshuai.scratch.flink.mini.util.concurrent;

import java.util.concurrent.CompletionException;

public class ExceptionUtils {
    public static Throwable stripCompletionException(Throwable throwable) {
        return stripException(throwable, CompletionException.class);
    }

    public static Throwable stripException(Throwable throwable, Class<? extends Throwable> typeToStrip) {
        while (typeToStrip.isAssignableFrom(throwable.getClass()) && throwable.getCause() != null) {
            throwable = throwable.getCause();
        }
        return throwable;
    }
}
