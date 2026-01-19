package cn.liboshuai.scratch.flink.mini;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class MiniInputGate {
    private final Queue<NetworkBuffer> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

    public void onBuffer(NetworkBuffer buffer) {
        lock.lock();
        try {
            queue.add(buffer);
            if (!availabilityFuture.isDone()) {
                availabilityFuture.complete(null);
            }
        } finally {
            lock.unlock();
        }
    }

    public NetworkBuffer pollNext() {
        lock.lock();
        try {
            NetworkBuffer buffer = queue.poll();
            if (queue.isEmpty()) {
                if (availabilityFuture.isDone()) {
                    availabilityFuture = new CompletableFuture<>();
                }
            }
            return buffer;
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<Void> getAvailabilityFuture() {
        lock.lock();
        try {
            return availabilityFuture;
        } finally {
            lock.unlock();
        }
    }
}
