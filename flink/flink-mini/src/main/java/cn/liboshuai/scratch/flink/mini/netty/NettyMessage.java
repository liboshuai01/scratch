package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.Getter;

/**
 * Flink 网络栈通信的统一消息基类及编解码器集合。
 * 这是读懂 Flink Netty 源码最重要的一张“蓝图”。
 */
public abstract class NettyMessage {

    // Frame Length (4) + Magic Number (4) + Msg ID (1)
    static final int FRAME_HEADER_LENGTH = 4 + 4 + 1;
    // Flink 的魔数，用来校验数据包合法性 (谐音 Bad Coffee)
    static final int MAGIC_NUMBER = 0xBADC0FFE;

    /**
     * 让子类自己决定如何将内容写入 ByteBuf 中。
     */
    abstract void write(ChannelHandlerContext ctx, ChannelPromise promise, ByteBufAllocator allocator) throws Exception;

    /**
     * 辅助分配器：预留了 Header 空间，并帮我们写好 Length 和 Magic。
     */
    protected static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
        int totalLength = FRAME_HEADER_LENGTH + contentLength;
        ByteBuf buffer = allocator.directBuffer(totalLength);

        // 1. 写入整个帧的总长度
        buffer.writeInt(totalLength);
        // 2. 写入魔数
        buffer.writeInt(MAGIC_NUMBER);
        // 3. 写入消息 ID
        buffer.writeByte(id);

        return buffer;
    }

    // =================================================================================
    //  子类：PartitionRequest (客户端向服务端请求拉取数据)
    // =================================================================================
    public static class PartitionRequest extends NettyMessage {
        static final byte ID = 2;
        final ResultPartitionID partitionId;
        final InputChannelID receiverId;
        final int credit; // 简化的流控 credit

        public PartitionRequest(ResultPartitionID partitionId, InputChannelID receiverId, int credit) {
            this.partitionId = partitionId;
            this.receiverId = receiverId;
            this.credit = credit;
        }

        @Override
        void write(ChannelHandlerContext ctx, ChannelPromise promise, ByteBufAllocator allocator) {
            int contentLength = ResultPartitionID.getByteBufLength() + InputChannelID.getByteBufLength() + Integer.BYTES;
            ByteBuf buf = allocateBuffer(allocator, ID, contentLength);

            partitionId.writeTo(buf);
            receiverId.writeTo(buf);
            buf.writeInt(credit);

            ctx.write(buf, promise); // 注意这里交由 context 写出
        }

        static PartitionRequest readFrom(ByteBuf buffer) {
            ResultPartitionID partitionId = ResultPartitionID.fromByteBuf(buffer);
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
            int credit = buffer.readInt();
            return new PartitionRequest(partitionId, receiverId, credit);
        }
    }

    // =================================================================================
    //  子类：BufferResponse (服务端向客户端发送真实数据缓冲)
    // =================================================================================
    public static class BufferResponse extends NettyMessage {
        static final byte ID = 0;
        final InputChannelID receiverId;
        final int sequenceNumber; // 包序号
        @Getter
        final ByteBuf buffer;     // 数据载体

        public BufferResponse(InputChannelID receiverId, int sequenceNumber, ByteBuf buffer) {
            this.receiverId = receiverId;
            this.sequenceNumber = sequenceNumber;
            this.buffer = buffer;
        }

        @Override
        void write(ChannelHandlerContext ctx, ChannelPromise promise, ByteBufAllocator allocator) {
            int headerLength = InputChannelID.getByteBufLength() + Integer.BYTES;
            // 数据的真实大小
            int dataLength = buffer.readableBytes();

            // 为了简易，我们这里合成分配一整块 Buffer（Flink 源码为了零拷贝会使用 CompositeByteBuf 或直接分别 write）
            ByteBuf outBuf = allocateBuffer(allocator, ID, headerLength + dataLength);

            receiverId.writeTo(outBuf);
            outBuf.writeInt(sequenceNumber);
            outBuf.writeBytes(buffer); // 把数据拷贝进去

            buffer.release(); // 使用完毕释放原始的包

            ctx.write(outBuf, promise);
        }

        static BufferResponse readFrom(ByteBuf in) {
            InputChannelID receiverId = InputChannelID.fromByteBuf(in);
            int seqNum = in.readInt();

            // 将剩余数据当做真实数据 Buffer (这需要分配新空间拷贝以避免 release 错误，简易实现直接 readBytes)
            ByteBuf dataBuf = in.alloc().buffer(in.readableBytes());
            in.readBytes(dataBuf);

            return new BufferResponse(receiverId, seqNum, dataBuf);
        }

    }

    // =================================================================================
    //  消息统一编码器 (ChannelOutboundHandlerAdapter)
    // =================================================================================
    public static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof NettyMessage) {
                // 委托给各个具体的消息类自己去 write
                ((NettyMessage) msg).write(ctx, promise, ctx.alloc());
            } else {
                ctx.write(msg, promise);
            }
        }
    }

    // =================================================================================
    //  消息统一解码器 (基于 LengthFieldBasedFrameDecoder 解决粘包)
    // =================================================================================
    public static class NettyMessageDecoder extends LengthFieldBasedFrameDecoder {
        public NettyMessageDecoder() {
            /*
             * Flink 经典的配置:
             * lengthFieldOffset = 0    (长度在开头)
             * lengthFieldLength = 4    (int占4字节)
             * lengthAdjustment = -4    (由于写入的 length 包含了自身长度，而 Netty 默认不包含，需要调整回去)
             * initialBytesToStrip = 4  (把 4 个字节的 length 字段剥离掉，丢弃)
             */
            super(Integer.MAX_VALUE, 0, 4, -4, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf msg = (ByteBuf) super.decode(ctx, in);
            if (msg == null) {
                return null;
            }

            try {
                // 读取剩余的 header
                int magicNumber = msg.readInt();
                if (magicNumber != MAGIC_NUMBER) {
                    throw new IllegalStateException("网络流已损坏，收到的魔数不正确！");
                }

                byte msgId = msg.readByte();

                // 委派给具体消息的 readFrom
                switch (msgId) {
                    case PartitionRequest.ID:
                        return PartitionRequest.readFrom(msg);
                    case BufferResponse.ID:
                        return BufferResponse.readFrom(msg);
                    default:
                        throw new IllegalStateException("收到未知消息类型 ID: " + msgId);
                }
            } finally {
                // super.decode 产生了一个 retain 的切片，这里用完必须释放
                msg.release();
            }
        }
    }
}