package cn.liboshuai.scratch.flink.mini.util.concurrent;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
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

    public static <T extends Throwable> Throwable firstOrSuppressed(T newException, @Nullable T previous) {
        Preconditions.checkNotNull(newException);
        if (previous == null || previous == newException) {
            return newException;
        } else {
            previous.addSuppressed(newException);
            return previous;
        }
    }
}
