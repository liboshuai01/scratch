package cn.liboshuai.scratch.flink.mini.configuration;

import java.io.IOException;

public interface IOReadableWritable {
    void write(DataOutputView out) throws IOException;
    void read(DataInputView in) throws IOException;
}
