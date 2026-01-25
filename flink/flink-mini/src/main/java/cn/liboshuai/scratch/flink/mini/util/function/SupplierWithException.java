package cn.liboshuai.scratch.flink.mini.util.function;

@FunctionalInterface
public interface SupplierWithException<R, E extends Throwable> {
    R get() throws E;
}
