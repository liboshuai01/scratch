package cn.liboshuai.scratch.tmp;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

/**
 * 简化的 InputChannelID，用于标识消费者端的接收通道。
 * 在 Flink 中，这通常由 16 个字节的 UUID 组成。
 */
public class InputChannelID {
    private final UUID uuid;

    public InputChannelID() {
        this(UUID.randomUUID());
    }

    public InputChannelID(UUID uuid) {
        this.uuid = uuid;
    }

    // 写入 ByteBuf
    public void writeTo(ByteBuf buf) {
        buf.writeLong(uuid.getMostSignificantBits());
        buf.writeLong(uuid.getLeastSignificantBits());
    }

    // 从 ByteBuf 读取
    public static InputChannelID fromByteBuf(ByteBuf buf) {
        long mostSigBits = buf.readLong();
        long leastSigBits = buf.readLong();
        return new InputChannelID(new UUID(mostSigBits, leastSigBits));
    }

    public static int getByteBufLength() {
        return 16; // 两个 long 占用 16 字节
    }

    @Override
    public String toString() {
        return uuid.toString();
    }
}
