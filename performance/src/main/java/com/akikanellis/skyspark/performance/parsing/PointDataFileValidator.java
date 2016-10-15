package com.akikanellis.skyspark.performance.parsing;

import com.akikanellis.skyspark.performance.result.PointDataFile;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

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
