package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class BiggerThanZeroIntegerValidator implements IParameterValidator {
    public void validate(String name, String value) throws ParameterException {
        int n = Integer.parseInt(value);
        if (n <= 0) {
            throw new ParameterException("Parameter " + name + " should be bigger than 0 (found " + value + ")");
        }
    }
}