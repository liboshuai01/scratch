package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.channel.ChannelHandler;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyProtocol
 * 工厂类，负责组装 Server 和 Client 的 Pipeline。
 */
public class NettyProtocol {

    private final MiniInputGate inputGate;

    public NettyProtocol(MiniInputGate inputGate) {
        this.inputGate = inputGate;
    }

    public ChannelHandler[] getServerChannelHandlers() {
        return new ChannelHandler[] {
                // 1. 编码器 (Outbound)
                new NettyMessage.NettyMessageEncoder(),
                // 2. 解码器 (Inbound) - 服务端也要接收 Request，所以也需要 Decoder
                // 为简化，服务端使用简单的 DecoderDelegate (实际 Flink 中服务端主要接收 Request)
                new NettyMessageClientDecoderDelegate(new NetworkBufferAllocator.SimpleAllocator()),
                // 3. 业务 Handler
                new PartitionRequestServerHandler()
        };
    }

    public ChannelHandler[] getClientChannelHandlers() {
        return new ChannelHandler[] {
                // 1. 编码器 (Outbound)
                new NettyMessage.NettyMessageEncoder(),
                // 2. 解码器 (Inbound) - 包含分层解码逻辑
                new NettyMessageClientDecoderDelegate(new NetworkBufferAllocator.SimpleAllocator()),
                // 3. 业务 Handler
                new CreditBasedPartitionRequestClientHandler(inputGate)
        };
    }
}