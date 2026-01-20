package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.PartitionRequestServerHandler
 * * 职责：
 * 1. 处理 PartitionRequest。
 * 2. 处理 AddCredit。
 * 3. 持有 OutboundQueue (这里简化为直接启动线程生成数据) 来发送 BufferResponse。
 */
@Slf4j
public class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    // 模拟 Flink 的 Reader 上下文
    private volatile boolean isStreaming = false;
    private final AtomicInteger availableCredit = new AtomicInteger(0);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        if (msg instanceof NettyMessage.PartitionRequest) {
            NettyMessage.PartitionRequest req = (NettyMessage.PartitionRequest) msg;
            log.info("[Server] 收到分区请求 Partition={}, InitialCredit={}", req.partitionId, req.credit);

            availableCredit.set(req.credit);

            if (!isStreaming) {
                isStreaming = true;
                startDataGenerator(ctx);
            }

        } else if (msg instanceof NettyMessage.AddCredit) {
            int delta = ((NettyMessage.AddCredit) msg).credit;
            int newCredit = availableCredit.addAndGet(delta);
            // log.debug("[Server] 收到 Credit 补充: +{}, 当前: {}", delta, newCredit);
        }
    }

    /**
     * 模拟 Flink 的 SubpartitionView 读取逻辑。
     * 在真实 Flink 中，这里是注册一个 Listener，当 Buffer 产生时触发发送。
     * 这里我们用线程模拟"生产-发送"循环，并加入了简单的 Credit 流控检查。
     */
    private void startDataGenerator(ChannelHandlerContext ctx) {
        new Thread(() -> {
            Random random = new Random();
            int seq = 0;
            try {
                while (ctx.channel().isActive()) {
                    // 简单的流控：如果没有 Credit，就等待
                    if (availableCredit.get() <= 0) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 模拟数据生产耗时
                    int sleep = random.nextInt(100) < 5 ? 200 : 10;
                    TimeUnit.MILLISECONDS.sleep(sleep);

                    // 构造数据
                    String payload = "Flink-Netty-Record-" + (++seq);
                    NetworkBuffer buffer = new NetworkBuffer(payload);

                    // 消耗 Credit
                    availableCredit.decrementAndGet();

                    // 发送
                    ctx.writeAndFlush(new NettyMessage.BufferResponse(buffer, seq, 0));
                }
            } catch (Exception e) {
                log.error("[Server] 数据生成线程异常", e);
            }
        }, "MiniFlink-DataProducer").start();
    }
}