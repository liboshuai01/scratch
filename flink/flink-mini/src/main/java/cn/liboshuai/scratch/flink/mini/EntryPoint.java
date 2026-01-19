package cn.liboshuai.scratch.flink.mini;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntryPoint {
    public static void main(String[] args) {
        log.info("=== MiniFlink (Netty Version) 启动 ===");
        int port = 9091;

        MiniInputGate inputGate = new MiniInputGate();

        NettyServer server = new NettyServer(port);
        new Thread(server::start).start();

        NettyClient client = new NettyClient("127.0.0.1", port, inputGate);
        client.start();

        // 4. 构建 Task
        try {
            log.info("[Main] 构建 Task 环境...");

            // [修改点] 这里直接使用功能更全的 CounterStreamTask (包含 Timer 演示)
            // 而不是使用 MyNettyStreamTask
            CounterStreamTask task = new CounterStreamTask(inputGate);

            // 启动 Checkpoint 调度器
            CheckpointScheduler cpScheduler = new CheckpointScheduler(task);
            cpScheduler.start();

            // 5. 启动 Task 主循环
            log.info("[Main] 开始执行 Task invoke...");
            task.invoke();

        } catch (Exception e) {
            log.error("Task 运行失败", e);
        } finally {
            client.shutdown();
            server.shutdown();
        }

    }
}
