package cn.liboshuai.scratch.flink.mini.netty;


/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NetworkBufferAllocator
 * 用于在 Netty 解码阶段分配 NetworkBuffer，承载接收到的数据。
 */
public interface NetworkBufferAllocator {

    /**
     * 分配一个用于接收数据的 Buffer。
     * 在 Flink 中这里会请求 MemorySegment，这里我们简化为创建一个包装了 ByteBuf 的 NetworkBuffer。
     */
    NetworkBuffer allocatePooledNetworkBuffer();

    /**
     * 简单的默认实现
     */
    class SimpleAllocator implements NetworkBufferAllocator {
        @Override
        public NetworkBuffer allocatePooledNetworkBuffer() {
            // 在 MiniFlink 中，NetworkBuffer 内部持有 byte[]
            // 为了配合 Netty 的 Zero-copy 写入，这里的实现稍作变通，
            // 实际解码器会填充数据到这个 Buffer 中。
            return new NetworkBuffer(new byte[0]);
        }
    }
}
