package cn.liboshuai.scratch.flink.mini;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.Getter;

public class NettyMessage {

    public static final int MAGIC_NUMBER = 0xBADC0FFE;

    public static final int FRAME_HEADER_LENGTH = 4 + 4 + 1;

    // 消息类型 ID
    public static final byte ID_PARTITION_REQUEST = 1;
    public static final byte ID_BUFFER_RESPONSE = 2;

    // 消息子类定义

    public static class PartitionRequest extends NettyMessage {
        public final int partitionId;
        public final int credit;

        public PartitionRequest(int partitionId, int credit) {
            this.partitionId = partitionId;
            this.credit = credit;
        }
    }

    @Getter
    public static class BufferResponse extends NettyMessage {

        private final NetworkBuffer buffer;

        public BufferResponse(NetworkBuffer buffer) {
            this.buffer = buffer;
        }

    }

    public static class MessageEncoder extends MessageToByteEncoder<NettyMessage> {

        @Override
        protected void encode(ChannelHandlerContext ctx, NettyMessage msg, ByteBuf out) throws Exception {
            // 1. 预留 4 个字节写 Frame Length
            int startIndex = out.writerIndex();
            out.writeInt(0);

            // 2. 写 Magic Number
            out.writeInt(NettyMessage.MAGIC_NUMBER);

            // 3. 写 Body
            if (msg instanceof PartitionRequest) {
                out.writeByte(ID_PARTITION_REQUEST);
                PartitionRequest partitionRequest = (PartitionRequest) msg;
                out.writeInt(partitionRequest.partitionId);
                out.writeInt(partitionRequest.credit);
            } else if (msg instanceof BufferResponse) {
                out.writeByte(ID_BUFFER_RESPONSE);
                BufferResponse bufferResponse = (BufferResponse) msg;
                byte[] data = bufferResponse.getBuffer().getBytes();
                out.writeInt(data.length);
                out.writeBytes(data);
            } else {
                throw new IllegalArgumentException("未知的消息类型: " + msg.getClass());
            }

            // 4. 回填 Frame Length
            int endIndex = out.writerIndex();
            out.setIndex(startIndex, endIndex - startIndex - 4);
        }
    }

    public static class MessageDecoder extends LengthFieldBasedFrameDecoder {

        public MessageDecoder() {
            super(Integer.MAX_VALUE, 0, 4, 0, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            try {
                // 验证魔数
                int magicNumber = frame.readInt();
                if (magicNumber != MAGIC_NUMBER) {
                    throw new CorruptedFrameException("魔数错误，网络流可能已经损坏。");
                }
                // 读取消息 ID
                byte msgId = frame.readByte();
                if (msgId == ID_PARTITION_REQUEST) {
                    int partitionId = frame.readInt();
                    int credit = frame.readInt();
                    return new PartitionRequest(partitionId, credit);
                } else if (msgId == ID_BUFFER_RESPONSE) {
                    int dataLength = frame.readInt();
                    byte[] data = new byte[dataLength];
                    frame.readBytes(data);
                    return new BufferResponse(new NetworkBuffer(data));
                } else {
                    throw new IllegalStateException("收到未知消息 ID: " + msgId);
                }
            } finally {
                frame.release();
            }
        }
    }

}
