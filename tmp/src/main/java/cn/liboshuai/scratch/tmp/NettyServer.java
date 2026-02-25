package cn.liboshuai.scratch.tmp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyServer {
    private final NettyConfig config;
    private final NettyProtocol protocol;

    private ServerBootstrap bootstrap;
    private ChannelFuture bindFuture;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;

    public NettyServer(NettyConfig config, NettyProtocol protocol) {
        this.config = config;
        this.protocol = protocol;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(config.getServerNumThreads());

        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(protocol.getServerChannelHandlers());
                    }
                });

        bindFuture = bootstrap.bind(config.getServerAddress(), config.getServerPort()).sync();
        log.info("Netty 服务端已启动，监听地址 {}:{}", config.getServerAddress(), config.getServerPort());
    }

    public void shutdown() {
        if (bindFuture != null) {
            bindFuture.channel().close().syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        log.info("Netty 服务端已关闭。");
    }
}
