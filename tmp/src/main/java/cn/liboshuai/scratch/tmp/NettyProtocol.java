package cn.liboshuai.scratch.tmp;

import io.netty.channel.ChannelHandler;

/**
 * 定义 Netty Server 和 Client 的 Pipeline 编排结构
 */
public class NettyProtocol {

    public ChannelHandler[] getServerChannelHandlers() {
        return new ChannelHandler[]{
                new NettyMessage.NettyMessageEncoder(),
                new NettyMessage.NettyMessageDecoder(),
                new PartitionRequestServerHandler()
        };
    }

    public ChannelHandler[] getClientChannelHandler() {
        return new ChannelHandler[]{
                new NettyMessage.NettyMessageEncoder(),
                new NettyMessage.NettyMessageDecoder(),
                new PartitionRequestClientHandler()
        };
    }
}
