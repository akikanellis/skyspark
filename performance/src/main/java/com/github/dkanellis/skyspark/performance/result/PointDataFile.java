package com.github.dkanellis.skyspark.performance.result;

import javax.validation.constraints.NotNull;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Files.getNameWithoutExtension;

public class PointDataFile {

    public static final String DATA_TYPE_ALIAS_ANTICORRELATED = "ANTICOR";
    public static final String DATA_TYPE_ALIAS_CORRELATED = "CORREL";
    public static final String DATA_TYPE_ALIAS_UNIFORM = "UNIFORM";

    public static final String DATA_TYPE_NAME_ANTICORRELATED = "Anticorrelated";
    public static final String DATA_TYPE_NAME_CORRELATED = "Correlated";
    public static final String DATA_TYPE_NAME_UNIFORM = "Uniform";

    private final String filePath;
    private final String dataType;
    private final int dataSize;

    private PointDataFile(@NotNull String filePath, @NotNull String dataType, final int dataSize) {
        this.filePath = checkNotEmpty(filePath);
        this.dataType = checkNotEmpty(dataType);
        checkArgument(dataSize > 0);
        this.dataSize = dataSize;
    }

    public static PointDataFile fromFilePath(@NotNull String filePath) {
        String[] fileNameArray = getNameWithoutExtension(filePath).split("_");
        String dataType = resolveDataType(fileNameArray);
        final int dataSize = resolveDataSize(fileNameArray);

        return new PointDataFile(filePath, dataType, dataSize);
    }

    private static String resolveDataType(String[] fileNameArray) {
        String dataTypeAlias = fileNameArray[0];

        switch (dataTypeAlias) {
            case DATA_TYPE_ALIAS_ANTICORRELATED:
                return DATA_TYPE_NAME_ANTICORRELATED;
            case DATA_TYPE_ALIAS_CORRELATED:
                return DATA_TYPE_NAME_CORRELATED;
            case DATA_TYPE_ALIAS_UNIFORM:
                return DATA_TYPE_NAME_UNIFORM;
            default:
                throw new IllegalArgumentException("Wrong file name format, alias found: " + dataTypeAlias);
        }
    }

    private static int resolveDataSize(String[] fileNameArray) {
        return Integer.parseInt(fileNameArray[2]);
    }

    public String getFilePath() {
        return filePath;
    }

    public String getDataType() {
        return dataType;
    }

    public int getDataSize() {
        return dataSize;
    }
}
