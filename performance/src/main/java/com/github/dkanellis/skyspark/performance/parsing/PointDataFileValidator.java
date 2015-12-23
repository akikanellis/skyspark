package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import com.github.dkanellis.skyspark.performance.result.PointDataFile;

public class PointDataFileValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
        try {
            PointDataFile.fromFilePath(value);
        } catch (IllegalArgumentException ex) {
            throw new ParameterException("File is of illegal format: " + value);
        }
    }
}
