package cn.liboshuai.scratch.flink.mini.util;

import java.io.PrintWriter;
import java.io.StringWriter;

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

    public static boolean isJvmFatalError(Throwable throwable) {
        return (throwable instanceof InternalError
                || throwable instanceof UnknownError
                || throwable instanceof ThreadDeath);
    }
}
