package com.akikanellis.skyspark.performance.parsing;

import com.akikanellis.skyspark.performance.result.PointDataFile;
import com.beust.jcommander.IStringConverter;

public class PointDataFileConverter implements IStringConverter<PointDataFile> {
    @Override
    public PointDataFile convert(String value) {
        return PointDataFile.fromFilePath(value);
    }
}
