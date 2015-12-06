package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import com.github.dkanellis.skyspark.performance.result.PointDataFile;

import java.io.File;

public class PointDataFileValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
        File file = new File(value);
        if (!file.exists()) {
            throw new ParameterException("File does not exist: " + file);
        } else if (!file.isFile()) {
            throw new ParameterException("File is invalid file: " + file);
        } else if (!file.canRead()) {
            throw new ParameterException("File is not readable: " + file);
        }

        try {
            PointDataFile.fromFilePath(value);
        } catch (IllegalArgumentException ex) {
            throw new ParameterException("File is of illegal format: " + file);
        }
    }
}
