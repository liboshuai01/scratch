package cn.liboshuai.scratch.flink.mini.configuration;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class ExecutionConfig {

    public static class GlobalJobParameters implements Serializable {

        private static final long serialVersionUID = 5546951980137599219L;

        public Map<String, String> toMap() {
            return Collections.emptyMap();
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && this.getClass() == obj.getClass();
        }
    }
}
