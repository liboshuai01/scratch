package cn.liboshuai.scratch.tmp;

import lombok.Getter;

/**
 * 简化版的 Netty 相关的配置参数
 */
@Getter
public class NettyConfig {
    private final String serverAddress;
    private final int serverPort;
    private final int serverNumThreads;
    private final int clientNumThreads;

    public NettyConfig(String serverAddress, int serverPort, int serverNumThreads, int clientNumThreads) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.serverNumThreads = serverNumThreads;
        this.clientNumThreads = clientNumThreads;
    }
}
