package cn.liboshuai.scratch.tmp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FutureUtils 的单元测试
 * 覆盖场景：
 * 1. 空集合边界情况
 * 2. 所有 Future 正常完成
 * 3. 单个/多个 Future 异常导致整体失败
 * 4. 进度 (Completed/Total) 统计准确性
 * 5. 异步并发场景
 */
class FutureUtilsTest {

    @Test
    @DisplayName("测试空集合：应该立即完成")
    void testEmptyFutures() {
        FutureUtils.WaitingConjunctFuture future = new FutureUtils.WaitingConjunctFuture(Collections.emptyList());

        assertTrue(future.isDone(), "空列表应该立即完成");
        assertFalse(future.isCompletedExceptionally(), "不应该有异常");
        assertDoesNotThrow(() -> future.get(), "获取结果不应抛出异常");
        assertEquals(0, future.getNumFuturesTotal());
        assertEquals(0, future.getNumFuturesCompleted());
    }

    @Test
    @DisplayName("测试正常场景：所有 Future 成功完成")
    void testAllFuturesSuccess() throws ExecutionException, InterruptedException {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();

        List<CompletableFuture<?>> futures = Arrays.asList(f1, f2);
        FutureUtils.WaitingConjunctFuture conjunctFuture = new FutureUtils.WaitingConjunctFuture(futures);

        // 初始状态
        assertFalse(conjunctFuture.isDone());
        assertEquals(2, conjunctFuture.getNumFuturesTotal());
        assertEquals(0, conjunctFuture.getNumFuturesCompleted());

        // 完成第一个
        f1.complete("Hello");
        assertFalse(conjunctFuture.isDone());
        assertEquals(1, conjunctFuture.getNumFuturesCompleted());

        // 完成第二个
        f2.complete(100);
        assertTrue(conjunctFuture.isDone());
        assertFalse(conjunctFuture.isCompletedExceptionally());
        assertEquals(2, conjunctFuture.getNumFuturesCompleted());

        // 验证结果
        assertNull(conjunctFuture.get(), "WaitingConjunctFuture 的结果应该是 null");
    }

    @Test
    @DisplayName("测试异常场景：只要有一个 Future 失败，整体应立即失败")
    void testFutureFailure() {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<Integer> f2 = new CompletableFuture<>();

        FutureUtils.WaitingConjunctFuture conjunctFuture = new FutureUtils.WaitingConjunctFuture(Arrays.asList(f1, f2));

        RuntimeException ex = new RuntimeException("Flink Job Failed");

        // f1 失败
        f1.completeExceptionally(ex);

        // 验证整体是否立即失败
        assertTrue(conjunctFuture.isDone());
        assertTrue(conjunctFuture.isCompletedExceptionally());

        ExecutionException executionException = assertThrows(ExecutionException.class, conjunctFuture::get);
        assertEquals(ex, executionException.getCause());

        // 注意：根据你的代码逻辑，异常分支没有增加 numCompleted
        // 这里验证一下实现细节是否符合预期
        assertEquals(0, conjunctFuture.getNumFuturesCompleted(), "根据当前实现，异常完成不计入 numCompleted");
    }

    @Test
    @DisplayName("测试混合场景：异常发生后，其他 Future 继续完成不会改变结果")
    void testMixedSuccessAndFailure() {
        CompletableFuture<String> successFuture = new CompletableFuture<>();
        CompletableFuture<String> failFuture = new CompletableFuture<>();

        FutureUtils.WaitingConjunctFuture conjunctFuture = new FutureUtils.WaitingConjunctFuture(Arrays.asList(successFuture, failFuture));

        // 1. 触发异常
        failFuture.completeExceptionally(new RuntimeException("Error"));
        assertTrue(conjunctFuture.isCompletedExceptionally());

        // 2. 另一个 Future 随后成功完成
        successFuture.complete("Success");

        // 验证：状态依然是异常，且异常信息是第一次失败的那个
        assertTrue(conjunctFuture.isCompletedExceptionally());
        // 验证计数器：成功的那个依然会触发回调增加计数
        assertEquals(1, conjunctFuture.getNumFuturesCompleted(), "后续成功的 Future 依然会增加计数器");
    }

    @Test
    @DisplayName("测试并发场景：多线程异步完成")
    @Timeout(5) // 防止死锁测试一直卡住
    void testConcurrentCompletion() throws ExecutionException, InterruptedException {
        int count = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Integer>> futures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < count; i++) {
            futures.add(new CompletableFuture<>());
        }

        FutureUtils.WaitingConjunctFuture conjunctFuture = new FutureUtils.WaitingConjunctFuture(futures);

        // 异步完成所有 future
        AtomicBoolean hasError = new AtomicBoolean(false);
        for (int i = 0; i < count; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    // 模拟一点随机延迟
                    Thread.sleep((long) (Math.random() * 10));
                    futures.get(index).complete(index);
                } catch (Exception e) {
                    hasError.set(true);
                }
            });
        }

        // 等待主 Future 完成
        conjunctFuture.get();

        assertEquals(count, conjunctFuture.getNumFuturesTotal());
        assertEquals(count, conjunctFuture.getNumFuturesCompleted());
        assertFalse(hasError.get(), "多线程执行过程中不应抛出异常");

        executor.shutdown();
    }
}