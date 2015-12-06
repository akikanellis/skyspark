package com.github.dkanellis.skyspark.performance;

public interface ResultWriter {
    void writeResult(String algorithmName, final long elapsedMillis, String dataType, final int dataSize);
}
