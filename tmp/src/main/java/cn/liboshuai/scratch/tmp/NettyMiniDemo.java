package cn.liboshuai.scratch.tmp;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

/**
 * 完整模拟 Flink 中 Task 间数据交换（拉取）的简易流程：
 * 1. 启动服务与客户端
 * 2. 消费者构建拉取请求 PartitionRequest
 * 3. 消费者通过 Client Channel 向远端发出拉取
 * 4. 远端接受并推送 BufferResponse
 */
@Slf4j
public class NettyMiniDemo {
    public static void main(String[] args) throws InterruptedException {
        // 1. 初始化配置
        NettyConfig config = new NettyConfig("127.0.0.1", 9090, 2, 2);
        NettyConnectManager connectionManager = new NettyConnectManager(config);

        try {
            // 2. 启动服务端与客户端线程池
            connectionManager.start();

            // 3. 建立连接获取 Channel
            Channel clientChannel = connectionManager.createClientChannel();

            // 4. 模拟准备好消费者 ReceiverID 与 上游的 ParitionID
            InputChannelID receiverId = new InputChannelID();
            ResultPartitionID partitionId = new ResultPartitionID();
            log.info(">>> 准备发起数据请求：从分区 {} 拉取数据", partitionId);

            // 5. 构造请求并发送
            NettyMessage.PartitionRequest request = new NettyMessage.PartitionRequest(
                    partitionId,
                    receiverId,
                    10 // 初始 credit = 10
            );

            clientChannel.writeAndFlush(request);
            // 让主线程等一会，观察日志里 Handler 收发的过程
            Thread.sleep(3000);
        } finally {
            // 6. 关闭清理资源
            connectionManager.shutdown();
        }
    }
}
