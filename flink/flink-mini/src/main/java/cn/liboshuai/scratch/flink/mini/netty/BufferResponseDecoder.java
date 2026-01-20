package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;


/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.BufferResponseDecoder
 * 专门用于解码 BufferResponse。
 * 特点：分为两个阶段，先解消息头(Header)，再解数据体(Body)。允许数据跨多个 TCP 包到达。
 */
public class BufferResponseDecoder extends NettyMessageDecoder {

    private final NetworkBufferAllocator allocator;

    // 缓存 Message Header 的 Buffer (Sequence + Backlog + Size)
    private ByteBuf headerBuffer;

    // 当前正在构建的响应对象
    private NettyMessage.BufferResponse currentResponse;

    // 已经读取的数据体长度
    private int accumulatedBodyBytes;

    public BufferResponseDecoder(NetworkBufferAllocator allocator) {
        this.allocator = allocator;
    }

    public void onChannelActive(ChannelHandlerContext ctx) {
        // 12 字节 = Sequence(4) + Backlog(4) + Size(4)
        if (headerBuffer == null) {
            headerBuffer = ctx.alloc().directBuffer(12);
        }
    }

    @Override
    public DecodingResult onChannelRead(ByteBuf data) {
        // 阶段 1: 解码消息头 (Message Header)
        if (currentResponse == null) {
            // 尝试读取足够的字节到 headerBuffer
            int needed = 12 - headerBuffer.readableBytes();
            int toCopy = Math.min(needed, data.readableBytes());
            data.readBytes(headerBuffer, toCopy);

            if (headerBuffer.readableBytes() < 12) {
                return DecodingResult.notFinished();
            }

            // Header 读满了，解析它
            currentResponse = NettyMessage.BufferResponse.readFrom(headerBuffer);
            headerBuffer.clear();
            accumulatedBodyBytes = 0;
        }

        // 阶段 2: 解码数据体 (Body)
        if (currentResponse != null) {
            int bodySize = currentResponse.buffer.getSize(); // 这里的 Size 是从 Header 读出来的
            int remaining = bodySize - accumulatedBodyBytes;

            int toRead = Math.min(remaining, data.readableBytes());
            if (toRead > 0) {
                // 将数据读入 NetworkBuffer 的底层数组中
                // 注意：在真实 Flink 中是 composite buffer 或直接写入 native memory
                // 这里为了 Mini 模拟，我们直接写入 byte[]
                byte[] target = currentResponse.buffer.getBytes();
                data.readBytes(target, accumulatedBodyBytes, toRead);
                accumulatedBodyBytes += toRead;
            }

            if (accumulatedBodyBytes >= bodySize) {
                NettyMessage.BufferResponse fullMsg = currentResponse;
                currentResponse = null; // 重置状态，准备读下一条
                return DecodingResult.fullMessage(fullMsg);
            }
        }

        return DecodingResult.notFinished();
    }

    @Override
    public void close() {
        if (headerBuffer != null) {
            headerBuffer.release();
            headerBuffer = null;
        }
    }
}