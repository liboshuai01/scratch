package cn.liboshuai.scratch.flink.mini.netty;

import cn.liboshuai.scratch.flink.mini.task.MiniInputGate;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler
 * * 职责：
 * 1. 接收 BufferResponse，并将其放入 InputGate。
 * 2. 接收 ErrorResponse。
 * 3. 管理 Credit (简化版：这里我们简单地在连接建立时发送一次 Request)。
 */
@Slf4j
public class CreditBasedPartitionRequestClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private final MiniInputGate inputGate;

    public CreditBasedPartitionRequestClientHandler(MiniInputGate inputGate) {
        this.inputGate = inputGate;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("[Client] 通道激活，发送 PartitionRequest...");
        // 模拟 Flink: 发送 PartitionRequest，携带初始 Credit
        // 在真实 Flink 中，这一步通常由 PartitionRequestClientFactory 触发
        ctx.writeAndFlush(new NettyMessage.PartitionRequest(0, 10));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        if (msg instanceof NettyMessage.BufferResponse) {
            NettyMessage.BufferResponse response = (NettyMessage.BufferResponse) msg;

            // 将接收到的 Buffer 传递给 InputGate
            inputGate.onBuffer(response.buffer);

            // 模拟 Credit 机制：每收到一个 Buffer，就回送一个 AddCredit
            // 这样保证 Server 端知道我们有能力继续接收
            ctx.writeAndFlush(new NettyMessage.AddCredit(1));

        } else if (msg instanceof NettyMessage.ErrorResponse) {
            log.error("[Client] 收到服务端错误: {}", ((NettyMessage.ErrorResponse) msg).message);
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[Client] Handler 异常", cause);
        ctx.close();
    }
}