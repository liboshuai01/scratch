package cn.liboshuai.scratch.tmp;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
public class Demo {
    public static void main(String[] args) {
        Supplier<CompletableFuture<String>> factory = () -> {
            log.info("---supplier中的代码---");
            return CompletableFuture.supplyAsync(() -> "run supplier");
        };

        CompletableFuture<String> future1 = factory.get();
        CompletableFuture<String> future2 = factory.get();
        CompletableFuture<String> future3 = factory.get();
        future1.thenAccept(s -> log.info("获取到future1的结果: {}", s));
        future2.thenAccept(s -> log.info("获取到future2的结果: {}", s));
        future3.thenAccept(s -> log.info("获取到future3的结果: {}", s));
    }
}
