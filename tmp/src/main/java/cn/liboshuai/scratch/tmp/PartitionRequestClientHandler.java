package cn.liboshuai.scratch.tmp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 位于客户端的处理器：接收来自服务端的真实数据缓冲 BufferResponse。
 * （在 Flink 源码中对应 CreditBasedPartitionRequestClientHandler）
 */
@Slf4j
public class PartitionRequestClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        if (msg instanceof NettyMessage.BufferResponse) {
            NettyMessage.BufferResponse response = (NettyMessage.BufferResponse) msg;
            ByteBuf buffer = response.getBuffer();

            try {
                // 读取真实数据
                byte[] bytes = new byte[buffer.readableBytes()];
                buffer.readBytes(bytes);
                String dataStr = new String(bytes);
                log.info("客户端收到缓冲数据：SeqNum={}, ReceiverId={}, 数据内容=[{}]", response.sequenceNumber, response.receiverId, dataStr);
            } finally {
                // 数据消费完毕，记得释放 Netty Buffer 以防内存泄漏
                buffer.release();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("客户端发生异常", cause);
        ctx.close();
    }
}
