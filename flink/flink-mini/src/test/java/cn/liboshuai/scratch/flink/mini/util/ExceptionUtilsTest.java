package cn.liboshuai.scratch.flink.mini.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class ExceptionUtilsTest {

    // ========================================================================
    // 测试 rethrow(Throwable t) - 无自定义消息版本
    // ========================================================================

    @Test
    @DisplayName("测试 rethrow(Error): 应该直接抛出原 Error 对象")
    void testRethrow_Error() {
        // 准备一个 Error
        Error originalError = new OutOfMemoryError("Test OOM");

        // 执行断言
        Error thrown = assertThrows(Error.class, () -> ExceptionUtils.rethrow(originalError));

        // 验证抛出的就是原来的那个对象，没有被包装
        assertSame(originalError, thrown, "Error 应该被原样抛出");
    }

    @Test
    @DisplayName("测试 rethrow(RuntimeException): 应该直接抛出原 RuntimeException 对象")
    void testRethrow_RuntimeException() {
        // 准备一个 RuntimeException
        RuntimeException originalException = new IllegalArgumentException("Test RuntimeException");

        // 执行断言
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> ExceptionUtils.rethrow(originalException));

        // 验证抛出的就是原来的那个对象
        assertSame(originalException, thrown, "RuntimeException 应该被原样抛出");
    }

    @Test
    @DisplayName("测试 rethrow(CheckedException): 应该被包装成 RuntimeException")
    void testRethrow_CheckedException() {
        // 准备一个受检异常 (Checked Exception)
        IOException checkedException = new IOException("Test IOException");

        // 执行断言：这里期望捕获的是 RuntimeException
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> ExceptionUtils.rethrow(checkedException));

        // 验证包装逻辑
        assertNotSame(checkedException, thrown, "受检异常应该被包装为新对象");
        assertSame(checkedException, thrown.getCause(), "新异常的 Cause 应该是原始的受检异常");
        // new RuntimeException(t) 默认会将 t.toString() 作为 message
        assertTrue(thrown.getMessage().contains("Test IOException"));
    }

    // ========================================================================
    // 测试 rethrow(Throwable t, String parentMessage) - 带自定义消息版本
    // ========================================================================

    @Test
    @DisplayName("测试 rethrow(Error, msg): 消息参数应被忽略，直接抛出原 Error")
    void testRethrowWithMessage_Error() {
        Error originalError = new StackOverflowError("Original StackOverflow");
        String ignoredMessage = "This message should be ignored";

        Error thrown = assertThrows(Error.class, () -> ExceptionUtils.rethrow(originalError, ignoredMessage));

        assertSame(originalError, thrown);
        assertEquals("Original StackOverflow", thrown.getMessage());
        assertFalse(thrown.getMessage().contains(ignoredMessage), "Error 不应包含包装消息");
    }

    @Test
    @DisplayName("测试 rethrow(RuntimeException, msg): 消息参数应被忽略，直接抛出原异常")
    void testRethrowWithMessage_RuntimeException() {
        RuntimeException originalException = new IllegalStateException("Original State Error");
        String ignoredMessage = "This message should be ignored";

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> ExceptionUtils.rethrow(originalException, ignoredMessage));

        assertSame(originalException, thrown);
        assertEquals("Original State Error", thrown.getMessage());
    }

    @Test
    @DisplayName("测试 rethrow(CheckedException, msg): 应该被包装并包含自定义消息")
    void testRethrowWithMessage_CheckedException() {
        IOException checkedException = new IOException("Disk Full");
        String parentMessage = "Context info: Failed to write file";

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> ExceptionUtils.rethrow(checkedException, parentMessage));

        // 验证包装逻辑
        assertNotSame(checkedException, thrown);
        assertSame(checkedException, thrown.getCause(), "Cause 应该是原始受检异常");

        // 验证消息是否包含自定义的 parentMessage
        assertEquals(parentMessage, thrown.getMessage(), "抛出的异常消息应为传入的 parentMessage");
    }
}