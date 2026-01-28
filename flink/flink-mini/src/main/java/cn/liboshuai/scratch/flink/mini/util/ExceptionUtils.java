package cn.liboshuai.scratch.flink.mini.util;

import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

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

    public static void tryEnrichOutOfMemoryError(
            @Nullable Throwable root,
            @Nullable String jvmMetaspaceOomNewErrorMessage,
            @Nullable String jvmDirectOomNewErrorMessage,
            @Nullable String jvmHeapSpaceOomNewErrorMessage
    ) {
        updateDetailMessage(
                root,
                (Throwable t) -> {
                    if (isMetaspaceOutOfMemoryError(t)) {
                        return jvmMetaspaceOomNewErrorMessage;
                    } else if (isDirectOutOfMemoryError(t)) {
                        return jvmDirectOomNewErrorMessage;
                    } else if (isHeapSpaceOutOfMemoryError(t)) {
                        return jvmHeapSpaceOomNewErrorMessage;
                    }
                    return null;
                }
        );
    }

    private static void updateDetailMessage(
            @Nullable Throwable root, @Nullable Function<Throwable, String> throwableToMessage
    ) {
        if (root == null || throwableToMessage == null) {
            return;
        }
        Throwable it = root;
        while (it != null) {
            String newErrorMessage = throwableToMessage.apply(root);
            if (newErrorMessage != null) {
                updateDetailMessageOfThrowable(root, newErrorMessage);
            }
            it = root.getCause();
        }
    }

    private static void updateDetailMessageOfThrowable(@NonNull Throwable root, String newErrorMessage) {
        Field field;
        try {
            field = Throwable.class.getDeclaredField("detailMessage");
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(
                    "The JDK Throwable contains a detailMessage member. The Throwable class provided on the classpath does not which is why this exception appears.",
                    e);
        }
        field.setAccessible(true);
        try {
            field.set(root, newErrorMessage);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "The JDK Throwable contains a private detailMessage member that should be accessible through reflection. This is not the case for the Throwable class provided on the classpath.",
                    e);
        }
    }

    public static <T extends Throwable> Optional<T> findThrowable(Throwable throwable, Class<T> searchType) {
        if (throwable == null || searchType == null) {
            return Optional.empty();
        }
        Throwable t = throwable;
        while (t != null) {
            if (searchType.isAssignableFrom(t.getClass())) {
                return Optional.of(searchType.cast(t));
            }
            t = t.getCause();
        }
        return Optional.empty();
    }

    public static Optional<Throwable> findThrowable(Throwable throwable, Predicate<Throwable> predicate) {
        if (throwable == null || predicate == null) {
            return Optional.empty();
        }
        Throwable t = throwable;
        while (t != null) {
            if (predicate.test(t)) {
                return Optional.of(t);
            }
            t = t.getCause();
        }
        return Optional.empty();
    }
}
