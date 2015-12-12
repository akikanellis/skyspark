package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.test_utils.Rdds;
import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap.BitmapPointsMock;
import com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap.FullBitmapStructureMock;
import com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap.FullDimensionXBitmapStructureMock;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.awt.geom.Point2D;

import static junit.framework.Assert.assertTrue;

public class BitmapStructureTest extends BaseSparkTest {

    private BitmapStructure bitmapStructure;
    private FullBitmapStructureMock fullBitmapStructureMock;

    @Before
    public void setUp() {
        this.bitmapStructure = new BitmapStructure(BitmapPointsMock.get10Points(getSparkContextWrapper()), 4);
        this.fullBitmapStructureMock = new FullDimensionXBitmapStructureMock(getSparkContextWrapper());
    }


    @Test
    public void keepDistincts_andSortByAscendingOrder() {
        JavaRDD<Point2D> currentPointsRdd = BitmapPointsMock.get10Points(getSparkContextWrapper());
        JavaRDD<Double> expectedValues = fullBitmapStructureMock.getDistinctValuesSorted();

        JavaRDD<Double> actualValues = bitmapStructure.getDistinctSorted(currentPointsRdd, 1);

        assertTrue(Rdds.areEqual(expectedValues, actualValues));
    }

    @Test
    public void mapValuesByIndex() {
        JavaRDD<Double> currentPoints = fullBitmapStructureMock.getDistinctValuesSorted();
        JavaPairRDD<Long, Double> expectedPoints = fullBitmapStructureMock.getValuesIndexed();

        JavaPairRDD<Long, Double> actualPoints = bitmapStructure.mapWithIndex(currentPoints);

        assertTrue(Rdds.areEqual(expectedPoints, actualPoints));
    }
}