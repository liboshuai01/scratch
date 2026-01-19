package cn.liboshuai.scratch.flink.mini;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private final MiniInputGate inputGate;

    public NettyClientHandler(MiniInputGate inputGate) {
        this.inputGate = inputGate;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        if (msg instanceof NettyMessage.BufferResponse) {
            NettyMessage.BufferResponse buffer = (NettyMessage.BufferResponse) msg;
            inputGate.onBuffer(buffer.getBuffer());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("[Client] 连接建立，发送 PartitionRequest...");
        ctx.writeAndFlush(new NettyMessage.PartitionRequest(0, 2));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[Client] 异常", cause);
        ctx.close();
    }
}
