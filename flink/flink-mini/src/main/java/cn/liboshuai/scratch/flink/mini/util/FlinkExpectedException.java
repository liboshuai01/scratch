package cn.liboshuai.scratch.flink.mini.util;

public class FlinkExpectedException extends Exception{
    private static final long serialVersionUID = -7912364966978305007L;

    public FlinkExpectedException(String message) {
        super(message);
    }

    public FlinkExpectedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlinkExpectedException(Throwable cause) {
        super(cause);
    }
}
