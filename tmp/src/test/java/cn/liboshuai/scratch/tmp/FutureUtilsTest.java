package cn.liboshuai.scratch.tmp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class FutureUtilsTest {

    @Test
    @DisplayName("测试空集合：应该立即完成")
    void testWaitForAll_EmptyCollection() throws ExecutionException, InterruptedException {
        FutureUtils.ConjunctFuture<Void> result = FutureUtils.waitForAll(Collections.emptyList());

        assertTrue(result.isDone(), "空集合应立即标记为 Done");
        assertFalse(result.isCompletedExceptionally(), "空集合不应异常");
        assertNull(result.get(), "结果应为 null");
        assertEquals(0, result.getNumFuturesTotal());
        assertEquals(0, result.getNumFuturesCompleted());
    }

    @Test
    @DisplayName("测试所有Future正常完成：同步场景")
    void testWaitForAll_AllSuccess_ManualCompletion() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.waitForAll(Arrays.asList(f1, f2));

        // 初始状态
        assertFalse(result.isDone());
        assertEquals(2, result.getNumFuturesTotal());
        assertEquals(0, result.getNumFuturesCompleted());

        // 完成第一个
        f1.complete("Hello");
        assertFalse(result.isDone());
        assertEquals(1, result.getNumFuturesCompleted());

        // 完成第二个
        f2.complete(100);
        assertTrue(result.isDone());
        assertEquals(2, result.getNumFuturesCompleted());
        assertNull(result.get());
    }

    @Test
    @DisplayName("测试单个Future异常：结果应立即异常完成 (Fail-Fast)")
    void testWaitForAll_OneException() {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.waitForAll(Arrays.asList(f1, f2));

        RuntimeException ex = new RuntimeException("测试异常");
        f1.completeExceptionally(ex);

        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        // 验证抛出的异常是否包含原始异常
        ExecutionException executionException = assertThrows(ExecutionException.class, result::get);
        assertEquals(ex, executionException.getCause());
    }

    @Test
    @DisplayName("测试多个Future异常：只体现第一个捕获到的异常")
    void testWaitForAll_MultipleExceptions() {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.waitForAll(Arrays.asList(f1, f2));

        RuntimeException ex1 = new RuntimeException("异常1");
        RuntimeException ex2 = new RuntimeException("异常2");

        f1.completeExceptionally(ex1);
        f2.completeExceptionally(ex2); // 即使第二个也异常，结果状态应由第一个决定

        assertTrue(result.isCompletedExceptionally());
        ExecutionException thrown = assertThrows(ExecutionException.class, result::get);
        assertEquals(ex1, thrown.getCause());
    }

    @Test
    @DisplayName("测试并发异步场景")
    @Timeout(2)
        // 防止测试死锁，设置2秒超时
    void testWaitForAll_AsyncConcurrency() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            CompletableFuture<String> f1 = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "Task1";
            }, executor);

            CompletableFuture<String> f2 = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "Task2";
            }, executor);

            FutureUtils.ConjunctFuture<Void> result = FutureUtils.waitForAll(Arrays.asList(f1, f2));

            // 阻塞等待结果
            result.get();

            assertTrue(result.isDone());
            assertEquals(2, result.getNumFuturesCompleted());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    @DisplayName("测试传入已经完成的 Future")
    void testWaitForAll_AlreadyCompleted() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = CompletableFuture.completedFuture("Done");
        CompletableFuture<String> f2 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.waitForAll(Arrays.asList(f1, f2));

        // f1 已经完成，所以 numCompleted 应该至少是 1 (取决于回调执行速度，通常是立即的)
        // 注意：whenComplete 在当前线程立即执行回调
        assertEquals(1, result.getNumFuturesCompleted());
        assertFalse(result.isDone());

        f2.complete("Done2");
        assertTrue(result.isDone());
        assertNull(result.get());
    }

    @Test
    @DisplayName("测试参数校验：传入Null应抛出NPE")
    void testWaitForAll_NullInput() {
        // 因为你使用了 Guava 的 checkNotNull
        assertThrows(NullPointerException.class, () -> FutureUtils.waitForAll(null));
    }

    @Test
    @DisplayName("测试混合类型：Future列表包含不同泛型")
    void testWaitForAll_MixedTypes() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = CompletableFuture.completedFuture("S");
        CompletableFuture<Double> f2 = CompletableFuture.completedFuture(1.0);
        CompletableFuture<Object> f3 = CompletableFuture.completedFuture(new Object());

        List<CompletableFuture<?>> list = Arrays.asList(f1, f2, f3);
        FutureUtils.ConjunctFuture<Void> result = FutureUtils.waitForAll(list);

        result.get();
        assertTrue(result.isDone());
        assertEquals(3, result.getNumFuturesTotal());
    }
}