package cn.liboshuai.scratch.tmp;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;

/**
 * 模仿Flink中的ExceptionUtils，以此来提升自己Java代码功底
 */
public class ExceptionUtils {

    public static <T extends Throwable> T firstOrSuppressed(T newThrowable, @Nullable T previous) {
        Preconditions.checkNotNull(newThrowable, "newException");

        if (previous == null || previous == newThrowable) {
            return newThrowable;
        } else {
            previous.addSuppressed(newThrowable);
            return previous;
        }
    }

    public static Throwable stripExecutionException(Throwable throwable) {
        return stripException(throwable, ExecutionException.class);
    }

    public static Throwable stripException(
            Throwable throwableToStrip, Class<? extends Throwable> typeToStrip) {
        while (typeToStrip.isAssignableFrom(throwableToStrip.getClass())
                && throwableToStrip.getCause() != null) {
            throwableToStrip = throwableToStrip.getCause();
        }

        return throwableToStrip;
    }

}
