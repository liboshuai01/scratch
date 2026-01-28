package cn.liboshuai.scratch.flink.mini.util;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;

public final class ExceptionUtils {

    private static final String STRINGIFIED_NULL_EXCEPTION = "(null)";

    public static String stringifyException(final Throwable throwable) {
        if (throwable == null) {
            return STRINGIFIED_NULL_EXCEPTION;
        }
        try {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            throwable.printStackTrace(printWriter);
            printWriter.close();
            return stringWriter.toString();
        } catch (Throwable e) {
            return e.getClass().getName() + " (error while printing stack trace)";
        }
    }

    public static boolean isJvmFatalError(Throwable t) {
        return (t instanceof InternalError
                || t instanceof UnknownError
                || t instanceof ThreadDeath);
    }

    public static boolean isJvmFatalOrOutOfMemoryError(Throwable t) {
        return isJvmFatalError(t) || (t instanceof OutOfMemoryError);
    }

    public static boolean isMetaspaceOutOfMemoryError(@Nullable Throwable t) {
        return isOutOfMemoryErrorWithMessageContaining(t, "Metaspace");
    }

    public static boolean isDirectOutOfMemoryError(@Nullable Throwable t) {
        return isOutOfMemoryErrorWithMessageContaining(t, "Direct buffer memory");
    }

    public static boolean isHeapSpaceOutOfMemoryError(@Nullable Throwable t) {
        return isOutOfMemoryErrorWithMessageContaining(t, "Java heap space");
    }

    private static boolean isOutOfMemoryErrorWithMessageContaining(@Nullable Throwable t, String infix) {
        return isOutOfMemoryError(t)
                && t.getMessage() != null
                && t.getMessage().toLowerCase(Locale.ROOT).contains(infix.toLowerCase(Locale.ROOT));
    }

    private static boolean isOutOfMemoryError(Throwable t) {
        return t != null && t.getClass() == OutOfMemoryError.class;
    }
}
