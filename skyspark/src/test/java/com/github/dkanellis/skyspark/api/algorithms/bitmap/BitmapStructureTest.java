package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.test_utils.Rdds;
import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap.FullBitmapStructureMock;
import com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap.FullDimensionXBitmapStructureMock;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.util.BitSet;

import static junit.framework.Assert.assertTrue;

public class BitmapStructureTest extends BaseSparkTest {

    private BitmapStructure bitmapStructure;
    private FullBitmapStructureMock fullBitmapStructureMock;

    @Before
    public void setUp() {
        this.fullBitmapStructureMock = new FullDimensionXBitmapStructureMock(getSparkContextWrapper());
        this.bitmapStructure = new BitmapStructure(fullBitmapStructureMock.getDimensionValues(), 4);
    }


    @Test
    public void keepDistincts_andSortByAscendingOrder() {
        JavaRDD<Double> expectedValues = fullBitmapStructureMock.getDistinctValuesSorted();

        JavaRDD<Double> actualValues = bitmapStructure.getDistinctSorted();

        assertTrue(Rdds.areEqual(expectedValues, actualValues));
    }

    @Test
    public void mapValuesByIndex() {
        JavaRDD<Double> currentPoints = fullBitmapStructureMock.getDistinctValuesSorted();
        JavaPairRDD<Double, Long> expectedPoints = fullBitmapStructureMock.getValuesIndexed();

        JavaPairRDD<Double, Long> actualPoints = bitmapStructure.mapWithIndex(currentPoints);

        assertTrue(Rdds.areEqual(expectedPoints, actualPoints));
    }

    @Test
    public void getBitSets() {
        JavaPairRDD<Double, Long> currentData = fullBitmapStructureMock.getValuesIndexed();
        JavaRDD<BitSet> expectedBitSets = fullBitmapStructureMock.getValuesBitSets();

        JavaRDD<BitSet> actualBitSets = bitmapStructure.calculateBitSets(currentData);

        assertTrue(Rdds.areEqual(expectedBitSets, actualBitSets));
    }
}