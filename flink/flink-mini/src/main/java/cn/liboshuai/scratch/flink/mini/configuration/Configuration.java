package cn.liboshuai.scratch.flink.mini.configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Configuration extends ExecutionConfig.GlobalJobParameters
        implements IOReadableWritable,
        Serializable,
        Cloneable,
        ReadableConfig,
        WritableConfig {
    private static final long serialVersionUID = -5945849636478209751L;

    private static final byte TYPE_STRING = 0;
    private static final byte TYPE_INT = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_BOOLEAN = 3;
    private static final byte TYPE_FLOAT = 4;
    private static final byte TYPE_DOUBLE = 5;
    private static final byte TYPE_BYTES = 6;

    private final Map<String, Object> confData;

    public Configuration() {
        this.confData = new HashMap<>();
    }

    public Configuration(Configuration other) {
        this.confData = new HashMap<>(other.confData);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        synchronized (this.confData) {
            out.writeInt(this.confData.size());
            for (Map.Entry<String, Object> entry : this.confData.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();
                Class<?> clazz = val.getClass();

                StringValue.writeString(key, out);
                if (clazz == String.class) {
                    out.write(TYPE_STRING);
                    StringValue.writeString((String) val, out);
                } else if (clazz == Integer.class) {
                    out.write(TYPE_INT);
                    out.writeInt((Integer) val);
                } else if (clazz == Long.class) {
                    out.write(TYPE_LONG);
                    out.writeLong((Long) val);
                } else if (clazz == Boolean.class) {
                    out.write(TYPE_BOOLEAN);
                    out.writeBoolean((Boolean) val);
                } else if (clazz == Float.class) {
                    out.write(TYPE_FLOAT);
                    out.writeFloat((Float) val);
                } else if (clazz == Double.class) {
                    out.write(TYPE_DOUBLE);
                    out.writeDouble((Double) val);
                } else if (clazz == byte[].class) {
                    out.write(TYPE_BYTES);
                    byte[] bytes = (byte[]) val;
                    out.writeInt(bytes.length);
                    out.write(bytes);
                } else {
                    throw new IllegalArgumentException(
                            "Unrecognized type. This method is deprecated and might not work"
                                    + " for all supported types.");
                }
            }
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        synchronized (this.confData) {
            final int numberOfProperties = in.readInt();
            for (int i = 0; i < numberOfProperties; i++) {
                String key = StringValue.readString(in);
                Object val;
                byte type = in.readByte();
                switch (type) {
                    case TYPE_STRING:
                        val = StringValue.readString(in);
                        break;
                    case TYPE_INT:
                        val = in.readInt();
                        break;
                    case TYPE_LONG:
                        val = in.readLong();
                        break;
                    case TYPE_BOOLEAN:
                        val = in.readBoolean();
                        break;
                    case TYPE_FLOAT:
                        val = in.readFloat();
                        break;
                    case TYPE_DOUBLE:
                        val = in.readDouble();
                        break;
                    case TYPE_BYTES:
                        int length = in.readInt();
                        byte[] bytes = new byte[length];
                        in.readFully(bytes);
                        val = bytes;
                        break;
                    default:
                        throw new IOException(
                                String.format(
                                        "Unrecognized type: %s. This method is deprecated and"
                                                + " might not work for all supported types.",
                                        type));
                }
                this.confData.put(key, val);
            }
        }
    }

    @Override
    public Configuration clone() {
        Configuration configuration = new Configuration();
        configuration.addAll(this);
        return configuration;
    }

    public void addAll(Configuration other) {
        synchronized (this.confData) {
            synchronized (other.confData) {
                confData.putAll(other.confData);
            }
        }
    }

    @Override
    public <T> T get(ConfigOption<T> option) {
        return null;
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return Optional.empty();
    }

    @Override
    public <T> WritableConfig set(ConfigOption<T> option, T value) {
        return null;
    }
}
