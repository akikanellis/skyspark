package com.akikanellis.skyspark.api.helpers;

import com.akikanellis.skyspark.api.test_utils.SparkContextRule;
import com.akikanellis.skyspark.api.test_utils.data_mocks.PointsMock;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Rule;
import org.junit.Test;

import java.awt.geom.Point2D;

import static org.junit.Assert.assertEquals;

public class TextFileToPointRDDTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    @Test
    public void getPointRDDFromTextFile() {
        String filePath = getClass().getResource("/UNIFORM_2_10.txt").getFile();
        String delimiter = " ";
        TextFileToPointRDD instance = new TextFileToPointRDD(sc.get());

        JavaRDD<Point2D> expResult = sc.parallelize(PointsMock.getUniform210());
        JavaRDD<Point2D> result = instance.getPointRddFromTextFile(filePath, delimiter);

        assertEquals(expResult.collect(), result.collect());
    }
}
