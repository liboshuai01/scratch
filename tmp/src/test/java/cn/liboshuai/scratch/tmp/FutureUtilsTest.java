package cn.liboshuai.scratch.tmp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collection;
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

    @Test
    @DisplayName("combineForAll: 测试空集合，应返回空列表")
    void testCombineForAll_EmptyCollection() throws ExecutionException, InterruptedException {
        FutureUtils.ConjunctFuture<Collection<String>> result = FutureUtils.combineForAll(Collections.emptyList());

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertTrue(result.get().isEmpty());
        assertEquals(0, result.getNumFuturesTotal());
    }

    @Test
    @DisplayName("combineForAll: 正常完成，验证结果顺序与输入顺序一致")
    void testCombineForAll_Success_OrderPreserved() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();

        // 输入顺序: f1, f2, f3
        FutureUtils.ConjunctFuture<Collection<String>> result = FutureUtils.combineForAll(Arrays.asList(f1, f2, f3));

        // 模拟乱序完成
        f2.complete("B"); // 第二个先完成
        assertEquals(1, result.getNumFuturesCompleted());
        assertFalse(result.isDone());

        f3.complete("C"); // 第三个完成
        assertEquals(2, result.getNumFuturesCompleted());

        f1.complete("A"); // 第一个最后完成
        assertTrue(result.isDone());

        // 验证: 结果列表必须保持 [A, B, C] 的顺序
        Collection<String> list = result.get();
        assertIterableEquals(Arrays.asList("A", "B", "C"), list, "结果列表顺序应与输入 Future 顺序一致");
    }

    @Test
    @DisplayName("combineForAll: 单个Future异常，结果应立即异常完成 (Fail-Fast)")
    void testCombineForAll_Exception() {
        CompletableFuture<Integer> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Collection<Integer>> result = FutureUtils.combineForAll(Arrays.asList(f1, f2));

        RuntimeException ex = new RuntimeException("测试异常");
        // 让第一个 Future 失败
        f1.completeExceptionally(ex);

        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        ExecutionException executionException = assertThrows(ExecutionException.class, result::get);
        assertEquals(ex, executionException.getCause());
    }

    @Test
    @DisplayName("combineForAll: 传入已完成的 Future")
    void testCombineForAll_AlreadyCompleted() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = CompletableFuture.completedFuture("Result1");
        CompletableFuture<String> f2 = CompletableFuture.completedFuture("Result2");

        FutureUtils.ConjunctFuture<Collection<String>> result = FutureUtils.combineForAll(Arrays.asList(f1, f2));

        assertTrue(result.isDone());
        assertEquals(2, result.getNumFuturesTotal());
        assertIterableEquals(Arrays.asList("Result1", "Result2"), result.get());
    }

    @Test
    @DisplayName("combineForAll: 参数校验，传入 Null 应抛出 NPE")
    void testCombineForAll_NullInput() {
        assertThrows(NullPointerException.class, () -> FutureUtils.combineForAll(null));
    }

    @Test
    @DisplayName("combineForAll: 并发测试")
    @Timeout(2)
    void testCombineForAll_AsyncConcurrency() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            CompletableFuture<Integer> f1 = CompletableFuture.supplyAsync(() -> {
                try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                return 1;
            }, executor);

            CompletableFuture<Integer> f2 = CompletableFuture.supplyAsync(() -> {
                try { Thread.sleep(50); } catch (InterruptedException ignored) {}
                return 2;
            }, executor);

            FutureUtils.ConjunctFuture<Collection<Integer>> result = FutureUtils.combineForAll(Arrays.asList(f1, f2));

            Collection<Integer> list = result.get();

            assertTrue(result.isDone());
            assertEquals(2, result.getNumFuturesCompleted());
            // 同样验证顺序
            assertIterableEquals(Arrays.asList(1, 2), list);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    @DisplayName("completedForAll: 测试空集合，应立即完成")
    void testCompletedForAll_EmptyCollection() throws ExecutionException, InterruptedException {
        FutureUtils.ConjunctFuture<Void> result = FutureUtils.completedForAll(Collections.emptyList());

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertNull(result.get());
    }

    @Test
    @DisplayName("completedForAll: 所有任务成功，结果应正常完成")
    void testCompletedForAll_AllSuccess() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.completedForAll(Arrays.asList(f1, f2));

        f1.complete("A");
        assertFalse(result.isDone(), "仅完成部分任务时，整体不应完成");

        f2.complete(1);
        assertTrue(result.isDone());
        assertNull(result.get());
    }

    @Test
    @DisplayName("completedForAll: 核心特性测试 - 发生异常时不立即返回，需等待所有任务结束")
    void testCompletedForAll_WaitFailure() {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.completedForAll(Arrays.asList(f1, f2));

        RuntimeException ex = new RuntimeException("F1 Failed");

        // 1. 让第一个任务失败
        f1.completeExceptionally(ex);

        // 【关键点】与 waitForAll 不同，这里不能立即完成（isDone 应为 false）
        assertFalse(result.isDone(), "completedForAll 即使遇到异常，也必须等待其他 Future 结束");

        // 2. 让第二个任务成功
        f2.complete("Success");

        // 3. 现在所有任务都结束了，整体状态应为完成，且为异常状态
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        ExecutionException executionException = assertThrows(ExecutionException.class, result::get);
        assertEquals(ex, executionException.getCause());
    }

    @Test
    @DisplayName("completedForAll: 异常聚合测试 - 多个异常应被 Suppressed")
    void testCompletedForAll_ExceptionAggregation() {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.completedForAll(Arrays.asList(f1, f2, f3));

        RuntimeException ex1 = new RuntimeException("Primary Exception");
        RuntimeException ex2 = new RuntimeException("Suppressed Exception");

        // 1. F1 失败
        f1.completeExceptionally(ex1);
        assertFalse(result.isDone());

        // 2. F2 失败
        f2.completeExceptionally(ex2);
        assertFalse(result.isDone());

        // 3. F3 成功
        f3.complete("Success");

        // 4. 所有结束，验证结果
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        ExecutionException executionException = assertThrows(ExecutionException.class, result::get);
        Throwable actualCause = executionException.getCause();

        // 验证主异常是第一个抛出的 ex1
        assertEquals(ex1, actualCause);

        // 验证 ex2 被添加到了 suppressed 列表中 (依赖 ExceptionUtils 逻辑)
        assertEquals(1, actualCause.getSuppressed().length, "应该有一个被抑制的异常");
        assertEquals(ex2, actualCause.getSuppressed()[0], "被抑制的异常应该是 ex2");
    }

    @Test
    @DisplayName("completedForAll: 混合已完成和未完成的 Future")
    void testCompletedForAll_MixedState() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = CompletableFuture.completedFuture("Done");
        CompletableFuture<String> f2 = new CompletableFuture<>(); // 未完成
        CompletableFuture<String> f3 = new CompletableFuture<>();
        f3.completeExceptionally(new RuntimeException("Oops")); // 已完成但异常

        FutureUtils.ConjunctFuture<Void> result = FutureUtils.completedForAll(Arrays.asList(f1, f2, f3));

        // 因为 f2 还没完，所以整体没完
        assertFalse(result.isDone());

        // 完成 f2
        f2.complete("Now Done");

        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally()); // 因为 f3 有异常
    }
}