package com.github.dkanellis.skyspark.performance;

import javax.validation.constraints.NotNull;

public interface ResultWriter {
    void writeResult(@NotNull Result result);
}
