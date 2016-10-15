package com.akikanellis.skyspark.performance.result;

import javax.validation.constraints.NotNull;

public interface ResultWriter {
    void writeResult(@NotNull Result result);
}
