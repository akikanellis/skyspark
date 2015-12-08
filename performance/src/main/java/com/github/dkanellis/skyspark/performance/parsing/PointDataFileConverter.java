package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IStringConverter;
import com.github.dkanellis.skyspark.performance.result.PointDataFile;

public class PointDataFileConverter implements IStringConverter<PointDataFile> {
    @Override
    public PointDataFile convert(String value) {
        return PointDataFile.fromFilePath(value);
    }
}
