package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NonBufferResponseDecoder
 * 处理简单的控制消息（PartitionRequest, AddCredit, Error 等）。
 * 这些消息通常很短，我们直接读取全部字节后解析。
 */
public class NonBufferResponseDecoder extends NettyMessageDecoder {

    private ByteBuf accumulationBuf;

    public void onChannelActive(ChannelHandlerContext ctx) {
        if (accumulationBuf == null) {
            accumulationBuf = ctx.alloc().directBuffer(128); // 初始给个小空间
        }
    }

    @Override
    public void onNewMessageReceived(int msgId, int messageLength) {
        super.onNewMessageReceived(msgId, messageLength);
        accumulationBuf.clear();
        // 确保容量足够
        if (accumulationBuf.capacity() < messageLength) {
            accumulationBuf.capacity(messageLength);
        }
    }

    @Override
    public DecodingResult onChannelRead(ByteBuf data) {
        // 将收到的数据累积起来
        int needed = messageLength - accumulationBuf.readableBytes();
        int toCopy = Math.min(needed, data.readableBytes());
        data.readBytes(accumulationBuf, toCopy);

        if (accumulationBuf.readableBytes() < messageLength) {
            return DecodingResult.notFinished();
        }

        // 数据读齐了，开始根据 msgId 解析
        NettyMessage msg;
        switch (msgId) {
            case NettyMessage.ID_PARTITION_REQUEST:
                msg = NettyMessage.PartitionRequest.readFrom(accumulationBuf);
                break;
            case NettyMessage.ID_ERROR_RESPONSE:
                msg = NettyMessage.ErrorResponse.readFrom(accumulationBuf);
                break;
            case NettyMessage.ID_ADD_CREDIT:
                msg = NettyMessage.AddCredit.readFrom(accumulationBuf);
                break;
            default:
                throw new IllegalStateException("未知的消息 ID: " + msgId);
        }

        return DecodingResult.fullMessage(msg);
    }

    @Override
    public void close() {
        if (accumulationBuf != null) {
            accumulationBuf.release();
            accumulationBuf = null;
        }
    }
}