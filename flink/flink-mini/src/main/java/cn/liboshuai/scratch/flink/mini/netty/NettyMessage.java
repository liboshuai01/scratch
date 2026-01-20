package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyMessage
 * 定义网络传输协议。
 * * 协议格式:
 * +------------------+------------------+--------++----------------+
 * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
 * +------------------+------------------+--------++----------------+
 */
public abstract class NettyMessage {

    // Flink 经典魔数
    public static final int MAGIC_NUMBER = 0xBADC0FFE;
    // Frame 头部长度: Length(4) + Magic(4) + ID(1)
    public static final int FRAME_HEADER_LENGTH = 4 + 4 + 1;

    // --- 消息 ID 定义 ---
    public static final byte ID_BUFFER_RESPONSE = 0;
    public static final byte ID_ERROR_RESPONSE = 1;
    public static final byte ID_PARTITION_REQUEST = 2;
    public static final byte ID_ADD_CREDIT = 6; // 对应 Flink 的 AddCredit

    abstract void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) throws IOException;

    // ------------------------------------------------------------------------
    //  具体消息类型
    // ------------------------------------------------------------------------

    /**
     * 服务端 -> 客户端：发送数据
     */
    public static class BufferResponse extends NettyMessage {
        public final NetworkBuffer buffer;
        public final int sequenceNumber;
        public final int backlog; // 积压量，用于 Credit 机制

        public BufferResponse(NetworkBuffer buffer, int sequenceNumber, int backlog) {
            this.buffer = buffer;
            this.sequenceNumber = sequenceNumber;
            this.backlog = backlog;
        }

        boolean isBuffer() {
            return true;
        }

        @Override
        void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
            byte[] data = buffer.getBytes();
            int dataLen = data.length;

            // 1. 分配 Header Buffer
            // Header: [Sequence(4)] + [Backlog(4)] + [Size(4)] = 12 bytes
            int headerLen = 4 + 4 + 4;
            ByteBuf headerBuf = allocator.directBuffer(FRAME_HEADER_LENGTH + headerLen);

            // 2. 写 Frame Header
            headerBuf.writeInt(FRAME_HEADER_LENGTH + headerLen + dataLen - 4); // Frame Length (不包含自身4字节)
            headerBuf.writeInt(MAGIC_NUMBER);
            headerBuf.writeByte(ID_BUFFER_RESPONSE);

            // 3. 写 Message Header
            headerBuf.writeInt(sequenceNumber);
            headerBuf.writeInt(backlog);
            headerBuf.writeInt(dataLen);

            // 4. 发送 Header
            ctx.write(headerBuf);

            // 5. 发送 Body (Zero-Copy: 直接 wrap 字节数组)
            ByteBuf dataBuf = io.netty.buffer.Unpooled.wrappedBuffer(data);
            ctx.write(dataBuf, promise);
        }

        // 静态读取方法 (仅读取 Message Header 部分，Body 由 Decoder 处理)
        public static BufferResponse readFrom(ByteBuf buffer) {
            int seq = buffer.readInt();
            int backlog = buffer.readInt();
            int size = buffer.readInt();
            // 注意：这里返回的 BufferResponse 暂时还没有数据，数据由 Decoder 填充
            // 我们用一个空的 NetworkBuffer 占位，或者用专门的 Builder
            return new BufferResponse(new NetworkBuffer(new byte[size]), seq, backlog);
        }
    }

    /**
     * 客户端 -> 服务端：请求分区
     */
    public static class PartitionRequest extends NettyMessage {
        public final int partitionId;
        public final int credit;

        public PartitionRequest(int partitionId, int credit) {
            this.partitionId = partitionId;
            this.credit = credit;
        }

        @Override
        void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
            ByteBuf buf = allocator.directBuffer();
            // PartitionId(4) + Credit(4)
            int msgLen = 4 + 4;

            buf.writeInt(FRAME_HEADER_LENGTH + msgLen - 4);
            buf.writeInt(MAGIC_NUMBER);
            buf.writeByte(ID_PARTITION_REQUEST);

            buf.writeInt(partitionId);
            buf.writeInt(credit);

            ctx.write(buf, promise);
        }

        public static PartitionRequest readFrom(ByteBuf buffer) {
            int pid = buffer.readInt();
            int credit = buffer.readInt();
            return new PartitionRequest(pid, credit);
        }
    }

    /**
     * 服务端 -> 客户端：错误响应
     */
    public static class ErrorResponse extends NettyMessage {
        public final String message;

        public ErrorResponse(Throwable cause) {
            this.message = cause.getMessage() == null ? cause.toString() : cause.getMessage();
        }

        public ErrorResponse(String message) {
            this.message = message;
        }

        @Override
        void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
            byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
            ByteBuf buf = allocator.directBuffer();

            buf.writeInt(FRAME_HEADER_LENGTH + 4 + bytes.length - 4);
            buf.writeInt(MAGIC_NUMBER);
            buf.writeByte(ID_ERROR_RESPONSE);

            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);

            ctx.write(buf, promise);
        }

        public static ErrorResponse readFrom(ByteBuf buffer) {
            int len = buffer.readInt();
            byte[] bytes = new byte[len];
            buffer.readBytes(bytes);
            return new ErrorResponse(new String(bytes, StandardCharsets.UTF_8));
        }
    }

    /**
     * 客户端 -> 服务端：增加信用 (AddCredit)
     */
    public static class AddCredit extends NettyMessage {
        public final int credit;

        public AddCredit(int credit) {
            this.credit = credit;
        }

        @Override
        void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
            ByteBuf buf = allocator.directBuffer();
            int msgLen = 4; // Credit(4)

            buf.writeInt(FRAME_HEADER_LENGTH + msgLen - 4);
            buf.writeInt(MAGIC_NUMBER);
            buf.writeByte(ID_ADD_CREDIT);
            buf.writeInt(credit);

            ctx.write(buf, promise);
        }

        public static AddCredit readFrom(ByteBuf buffer) {
            return new AddCredit(buffer.readInt());
        }
    }

    // ------------------------------------------------------------------------
    //  Encoder (所有消息通用的编码器)
    // ------------------------------------------------------------------------

    @ChannelHandler.Sharable
    public static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof NettyMessage) {
                ((NettyMessage) msg).write(ctx, msg, promise, ctx.alloc());
            } else {
                ctx.write(msg, promise);
            }
        }
    }
}