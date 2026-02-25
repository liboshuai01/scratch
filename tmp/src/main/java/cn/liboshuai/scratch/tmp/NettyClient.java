package cn.liboshuai.scratch.tmp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyClient {
    private final NettyConfig config;
    private final NettyProtocol protocol;

    private Bootstrap bootstrap;
    private NioEventLoopGroup clientGroup;

    public NettyClient(NettyConfig config, NettyProtocol protocol) {
        this.config = config;
        this.protocol = protocol;
    }

    public void start() {
        clientGroup = new NioEventLoopGroup(config.getClientNumThreads());

        bootstrap = new Bootstrap()
                .group(clientGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(protocol.getClientChannelHandlers());
                    }
                });

        log.info("Netty 客户端已就绪。");
    }

    public Channel connect() throws InterruptedException {
        ChannelFuture future = bootstrap.connect(config.getServerAddress(), config.getServerPort()).sync();
        return future.channel();
    }

    public void shutdown() {
        if (clientGroup != null) {
            clientGroup.shutdownGracefully();
        }
        log.info("Netty 客户端已关闭。");
    }
}
