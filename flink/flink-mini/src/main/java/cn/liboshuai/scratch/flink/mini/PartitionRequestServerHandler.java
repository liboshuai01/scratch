package cn.liboshuai.scratch.flink.mini;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        if (msg instanceof NettyMessage.PartitionRequest) {
            NettyMessage.PartitionRequest req = (NettyMessage.PartitionRequest) msg;
            log.info("[Server] 收到分区请求: PartitionId={}, InitialCredit={}", req.partitionId, req.credit);

            startDataGenerator(ctx);
        }
    }

    private void startDataGenerator(ChannelHandlerContext ctx) {
        new Thread(() -> {
            log.info("[Server] 开始向客户端发送数据流...");
            Random random = new Random();
            int seq = 0;
            try {
                while (ctx.channel().isActive()) {
                    int sleep = random.nextInt(100) < 5 ? 500 : 20;
                    TimeUnit.MILLISECONDS.sleep(sleep);
                    String payload = "Netty-Record-" + ++seq;
                    NetworkBuffer networkBuffer = new NetworkBuffer(payload);
                    ctx.writeAndFlush(new NettyMessage.BufferResponse(networkBuffer));
                }
            } catch (Exception e) {
                log.error("[Server] 数据生成线程异常", e);
            }
            log.info("[Server] 数据发送结束");
        }
        , "MiniFlink-DataProducer").start();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[Server] 连接异常", cause);
        ctx.close();
    }
}
