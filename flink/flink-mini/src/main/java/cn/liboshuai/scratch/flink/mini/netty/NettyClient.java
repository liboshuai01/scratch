package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyClient {
    private final String host;
    private final int port;
    private final NettyProtocol protocol;
    private EventLoopGroup group;

    public NettyClient(String host, int port, NettyProtocol protocol) {
        this.host = host;
        this.port = port;
        this.protocol = protocol;
    }

    public void start() {
        group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(protocol.getClientChannelHandlers());
                        }
                    });

            ChannelFuture f = b.connect(host, port).sync();
            log.info("=== MiniFlink Netty Client 已连接到 {}:{} ===", host, port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}