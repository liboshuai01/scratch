package cn.liboshuai.scratch.flink.mini;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyServer {
    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workGroup;

    public NettyServer(int port) {
        this.port = port;
    }

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workGroup = new NioEventLoopGroup();
    }
}
