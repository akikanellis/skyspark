package com.github.dkanellis.skyspark.api.helpers;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.github.dkanellis.skyspark.api.testUtils.base.BaseSparkTest;
import com.github.dkanellis.skyspark.api.testUtils.categories.types.SparkTests;
import com.github.dkanellis.skyspark.api.testUtils.dataMocks.PointsMock;

import java.awt.geom.Point2D;

import static org.junit.Assert.assertEquals;

@Category(SparkTests.class)
public class TextFileToPointRDDTest extends BaseSparkTest {

    @Test
    public void getPointRDDFromTextFile() {
        String filePath = getClass().getResource("/UNIFORM_2_10.txt").getFile();
        String delimiter = " ";
        TextFileToPointRDD instance = new TextFileToPointRDD(getSparkContextWrapper());

        JavaRDD<Point2D> expResult = getSparkContextWrapper().parallelize(PointsMock.getUniform210());
        JavaRDD<Point2D> result = instance.getPointRDDFromTextFile(filePath, delimiter);

        assertEquals(expResult.collect(), result.collect());
    }
}
