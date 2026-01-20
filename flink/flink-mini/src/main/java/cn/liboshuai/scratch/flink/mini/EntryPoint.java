package cn.liboshuai.scratch.flink.mini;


import cn.liboshuai.scratch.flink.mini.checkpoint.CheckpointScheduler;
import cn.liboshuai.scratch.flink.mini.task.MiniInputGate;
import cn.liboshuai.scratch.flink.mini.netty.NettyClient;
import cn.liboshuai.scratch.flink.mini.netty.NettyProtocol;
import cn.liboshuai.scratch.flink.mini.netty.NettyServer;
import cn.liboshuai.scratch.flink.mini.task.CounterStreamTask;
import lombok.extern.slf4j.Slf4j;

/**
 * 新的启动类，集成了 Netty 网络通信模块。
 * 流程：
 * 1. 启动 Netty Server (模拟上游 TM)
 * 2. 启动 Netty Client (模拟本 TM 网络层)，并关联 InputGate
 * 3. 启动 Task 主线程，从 InputGate 消费 Netty 传来的数据
 */
@Slf4j
public class EntryPoint {

    public static void main(String[] args) {
        log.info("=== MiniFlink (Netty Version) 启动 ===");

        int port = 9091;

        // 在 EntryPoint 中使用的新方式：
        MiniInputGate inputGate = new MiniInputGate();
        NettyProtocol protocol = new NettyProtocol(inputGate);

        NettyServer server = new NettyServer(port, protocol);
        new Thread(server::start).start();
        NettyClient client = new NettyClient("127.0.0.1", port, protocol);
        client.start();

        // 4. 构建 Task
        try {
            log.info("[Main] 构建 Task 环境...");

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

