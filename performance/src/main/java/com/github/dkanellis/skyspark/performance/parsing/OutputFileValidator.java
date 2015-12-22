package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;

@SuppressWarnings("FieldCanBeLocal")
public class OutputFileValidator implements IParameterValidator {

    private static String EXTENSION_TEXT_FILE = "txt";
    private static String EXTENSION_EXCEL = "xls";

    @Override
    public void validate(String name, String value) throws ParameterException {
        String fileExtension = Files.getFileExtension(value);
        if (!(fileExtension.equals(EXTENSION_TEXT_FILE) || fileExtension.equals(EXTENSION_EXCEL))) {
            throw new ParameterException("File is of wrong type: " + fileExtension);
        }
    }
}
