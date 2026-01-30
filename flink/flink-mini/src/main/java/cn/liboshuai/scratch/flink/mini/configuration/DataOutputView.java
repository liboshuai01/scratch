package cn.liboshuai.scratch.flink.mini.configuration;

import java.io.DataOutput;
import java.io.IOException;

public interface DataOutputView extends DataOutput {

    void skipBytesToWrite(int numBytes) throws IOException;

    void write(DataOutputView source, int numBytes) throws IOException;
}
