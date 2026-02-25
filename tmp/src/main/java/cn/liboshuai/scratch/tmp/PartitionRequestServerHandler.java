package cn.liboshuai.scratch.tmp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;


/**
 * 位于服务端的处理器：负责接收下游的 PartitionRequest，并开始推送数据。
 */
@Slf4j
public class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        if (msg instanceof NettyMessage.PartitionRequest) {
            NettyMessage.PartitionRequest request = (NettyMessage.PartitionRequest) msg;
            log.info("服务端收到数据拉取请求：Partition={}, Receiver={}", request.partitionID, request.receiverId);

            // 在真正的 Flink 中，这里会创建 ViewReader，挂载到 Queue 中。
            // 为了简易演示，我们立即模拟源源不断地回复 3 条数据
            for (int i = 0; i < 3; i++) {
                String payload = "Hello Flink Data Stream [" + i + "]";
                ByteBuf data = ctx.alloc().buffer();
                data.writeBytes(payload.getBytes());
                NettyMessage.BufferResponse response = new NettyMessage.BufferResponse(
                        request.receiverId,
                        i,
                        data
                );
                // 写回并刷新
                ctx.writeAndFlush(response);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("服务端发生异常", cause);
        ctx.close();
    }
}
