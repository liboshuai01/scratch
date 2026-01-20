package cn.liboshuai.scratch.flink.mini.netty;

import io.netty.buffer.ByteBuf;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyMessageDecoder
 * 特定消息解码器的基类。
 */
public abstract class NettyMessageDecoder {

    /** 正在解码的消息 ID */
    protected int msgId;

    /** 正在解码的消息总长度 (包含 Header 和 Body) */
    protected int messageLength;

    /**
     * 通知有新消息到达。
     * @param msgId 消息 ID
     * @param messageLength 消息长度
     */
    public void onNewMessageReceived(int msgId, int messageLength) {
        this.msgId = msgId;
        this.messageLength = messageLength;
    }

    /**
     * 处理读取到的数据片段。
     * @param data 接收到的 ByteBuf
     * @return 解码结果
     */
    public abstract DecodingResult onChannelRead(ByteBuf data) throws Exception;

    public abstract void close();

    /**
     * 解码结果封装
     */
    public static class DecodingResult {
        private final boolean finished;
        private final NettyMessage message;

        private DecodingResult(boolean finished, NettyMessage message) {
            this.finished = finished;
            this.message = message;
        }

        public static DecodingResult notFinished() {
            return new DecodingResult(false, null);
        }

        public static DecodingResult fullMessage(NettyMessage message) {
            return new DecodingResult(true, message);
        }

        public boolean isFinished() {
            return finished;
        }

        public NettyMessage getMessage() {
            return message;
        }
    }
}