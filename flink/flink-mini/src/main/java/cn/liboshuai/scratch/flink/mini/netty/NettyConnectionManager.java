package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.channel.Channel;

/**
 * 门面类：统一管理网络组件的启动和关闭。
 */
public class NettyConnectionManager {

    private final NettyServer server;
    private final NettyClient client;

    public NettyConnectionManager(NettyConfig config) {
        NettyProtocol protocol = new NettyProtocol();
        this.server = new NettyServer(config, protocol);
        this.client = new NettyClient(config, protocol);
    }

    public void start() throws InterruptedException {
        server.start();
        client.start();
    }

    public Channel createClientChannel() throws InterruptedException {
        return client.connect();
    }

    public void shutdown() {
        client.shutdown();
        server.shutdown();
    }
}