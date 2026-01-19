package cn.liboshuai.scratch.flink.mini;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyServer {
    private final int port;
    private EventLoopGroup boosGroup;
    private EventLoopGroup workGroup;


    public NettyServer(int port) {
        this.port = port;
    }

    public void start() {
        boosGroup = new NioEventLoopGroup(1);
        workGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(boosGroup, workGroup)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new NettyMessage.MessageDecoder());
                        ch.pipeline().addLast(new NettyMessage.MessageEncoder());
                        ch.pipeline().addLast(new PartitionRequestServerHandler());
                    }
                });
        try {
            serverBootstrap.bind(port).sync();
            log.info("=== MiniFlink Netty Server 启动在端口 {} ===", port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        if (boosGroup != null) {
            boosGroup.shutdownGracefully();
        }
        if (workGroup != null) {
            workGroup.shutdownGracefully();
        }
    }
}
