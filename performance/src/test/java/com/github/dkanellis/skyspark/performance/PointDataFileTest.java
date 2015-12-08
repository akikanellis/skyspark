package com.github.dkanellis.skyspark.performance;

import com.github.dkanellis.skyspark.performance.result.PointDataFile;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PointDataFileTest {

    @Test
    public void anticorFile_giveAnticorrelatedDataType() {
        String filePath = "/home/aki/Projects/SkySpark/performance/data/datasets/big/ANTICOR_2_1000000.txt";
        String expectedName = PointDataFile.DATA_TYPE_NAME_ANTICORRELATED;

        PointDataFile pointDataFile = PointDataFile.fromFilePath(filePath);
        String actualName = pointDataFile.getDataType();

        assertEquals(expectedName, actualName);
    }

    @Test
    public void correlFile_giveCorrelatedDataType() {
        String filePath = "/home/aki/Projects/SkySpark/performance/data/datasets/big/CORREL_2_1000000.txt";
        String expectedName = PointDataFile.DATA_TYPE_NAME_CORRELATED;

        PointDataFile pointDataFile = PointDataFile.fromFilePath(filePath);
        String actualName = pointDataFile.getDataType();

        assertEquals(expectedName, actualName);
    }

    @Test
    public void uniformFile_giveUniformDataType() {
        String filePath = "/home/aki/Projects/SkySpark/performance/data/datasets/big/UNIFORM_2_1000000.txt";
        String expectedName = PointDataFile.DATA_TYPE_NAME_UNIFORM;

        PointDataFile pointDataFile = PointDataFile.fromFilePath(filePath);
        String actualName = pointDataFile.getDataType();

        assertEquals(expectedName, actualName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongFormattedFile_throwException() {
        String filePath = "/home/aki/Projects/SkySpark/performance/data/datasets/big/WRONG.txt";

        PointDataFile pointDataFile = PointDataFile.fromFilePath(filePath);
    }

    @Test
    public void numberOnFile_giveCorrectDataSize() {
        String filePath = "/home/aki/Projects/SkySpark/performance/data/datasets/big/UNIFORM_2_1000000.txt";
        final int expectedSize = 1000000;

        PointDataFile pointDataFile = PointDataFile.fromFilePath(filePath);
        final int actualSize = pointDataFile.getDataSize();

        assertEquals(expectedSize, actualSize);
    }
}