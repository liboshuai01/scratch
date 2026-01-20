package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyMessageClientDecoderDelegate
 * * 这是一个 Netty Inbound Handler，负责：
 * 1. 解码 Frame Header (Length + Magic + ID)。
 * 2. 根据 ID 将剩余数据的解码工作委托给 `BufferResponseDecoder` 或 `NonBufferResponseDecoder`。
 */
@Slf4j
public class NettyMessageClientDecoderDelegate extends ChannelInboundHandlerAdapter {

    private final BufferResponseDecoder bufferResponseDecoder;
    private final NonBufferResponseDecoder nonBufferResponseDecoder;

    // Frame Header 缓存
    private ByteBuf frameHeaderBuffer;

    // 当前正在使用的解码器
    private NettyMessageDecoder currentDecoder;

    public NettyMessageClientDecoderDelegate(NetworkBufferAllocator allocator) {
        this.bufferResponseDecoder = new BufferResponseDecoder(allocator);
        this.nonBufferResponseDecoder = new NonBufferResponseDecoder();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        frameHeaderBuffer = ctx.alloc().directBuffer(NettyMessage.FRAME_HEADER_LENGTH);
        bufferResponseDecoder.onChannelActive(ctx);
        nonBufferResponseDecoder.onChannelActive(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf data = (ByteBuf) msg;
        try {
            while (data.isReadable()) {
                // 1. 如果有选定的解码器，先让它跑
                if (currentDecoder != null) {
                    NettyMessageDecoder.DecodingResult result = currentDecoder.onChannelRead(data);
                    if (result.isFinished()) {
                        // 解码完成，将对象向下传递
                        ctx.fireChannelRead(result.getMessage());
                        currentDecoder = null;
                        frameHeaderBuffer.clear();
                    } else {
                        // 数据不够，等下一次 channelRead
                        break;
                    }
                }

                // 2. 如果没有在解码 Body，说明需要解 Frame Header
                if (currentDecoder == null) {
                    int needed = NettyMessage.FRAME_HEADER_LENGTH - frameHeaderBuffer.readableBytes();
                    int toCopy = Math.min(needed, data.readableBytes());
                    data.readBytes(frameHeaderBuffer, toCopy);

                    if (frameHeaderBuffer.readableBytes() == NettyMessage.FRAME_HEADER_LENGTH) {
                        // Header 读齐了，解析它
                        int frameLength = frameHeaderBuffer.readInt(); // 总帧长
                        int magic = frameHeaderBuffer.readInt();
                        byte msgId = frameHeaderBuffer.readByte();

                        if (magic != NettyMessage.MAGIC_NUMBER) {
                            throw new IllegalStateException("魔数错误，连接可能已损坏");
                        }

                        // 消息体长度 = 总帧长 - (Magic 4 + ID 1)
                        int messageBodyLength = frameLength - 5;

                        // 选择解码器
                        if (msgId == NettyMessage.ID_BUFFER_RESPONSE) {
                            currentDecoder = bufferResponseDecoder;
                        } else {
                            currentDecoder = nonBufferResponseDecoder;
                        }

                        // 初始化解码器状态
                        currentDecoder.onNewMessageReceived(msgId, messageBodyLength);
                    } else {
                        // Header 没读齐，跳出等待更多数据
                        break;
                    }
                }
            }
        } finally {
            data.release();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (frameHeaderBuffer != null) frameHeaderBuffer.release();
        bufferResponseDecoder.close();
        nonBufferResponseDecoder.close();
        super.channelInactive(ctx);
    }
}