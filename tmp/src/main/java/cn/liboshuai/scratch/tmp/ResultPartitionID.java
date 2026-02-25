package cn.liboshuai.scratch.tmp;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

/**
 * 简化的 ResultPartitionID，用于标识生产者端的数据分区。
 */
public class ResultPartitionID {
    private final UUID uuid;

    public ResultPartitionID() {
        this(UUID.randomUUID());
    }

    public ResultPartitionID(UUID uuid) {
        this.uuid = uuid;
    }

    // 写入 ByteBuf
    public void writeTo(ByteBuf buf) {
        buf.writeLong(uuid.getMostSignificantBits());
        buf.writeLong(uuid.getLeastSignificantBits());
    }

    // 从 ByteBuf 读取
    public static ResultPartitionID fromByteBuf(ByteBuf buf) {
        long mostSigBits = buf.readLong();
        long leastSigBits = buf.readLong();
        return new ResultPartitionID(new UUID(mostSigBits, leastSigBits));
    }

    public static int getByteBufLength() {
        return 16;
    }

    @Override
    public String toString() {
        return uuid.toString();
    }
}
