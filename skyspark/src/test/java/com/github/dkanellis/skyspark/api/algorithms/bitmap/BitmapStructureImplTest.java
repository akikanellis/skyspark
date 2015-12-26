package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.test_utils.Rdds;
import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap.FullBitmapStructureMock;
import com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap.FullDimensionXBitmapStructureMock;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static junit.framework.Assert.assertTrue;

public class BitmapStructureImplTest extends BaseSparkTest {

    private BitmapCalculator bitmapStructure;
    private FullBitmapStructureMock fullBitmapStructureMock;

    @Before
    public void setUp() {
        this.fullBitmapStructureMock = new FullDimensionXBitmapStructureMock(getSparkContext());
        this.bitmapStructure = new BitmapCalculator(4, getSparkContext());
    }

    @Test
    public void fullRun() {
        List<Point2D> pointsList = Arrays.asList(
                new Point2D.Double(5.4, 4.4),
                new Point2D.Double(5.0, 4.1),
                new Point2D.Double(3.6, 9.0),
                new Point2D.Double(5.9, 4.0),
                new Point2D.Double(5.9, 4.6),
                new Point2D.Double(2.5, 7.3),
                new Point2D.Double(6.3, 3.5),
                new Point2D.Double(9.9, 4.1),
                new Point2D.Double(6.7, 3.3),
                new Point2D.Double(6.1, 3.4)
        );

        JavaRDD<Point2D> points = getSparkContext().parallelize(pointsList);

        bitmapStructure.computeBitSlices(points).collect();
    }


    @Test
    public void keepDistincts_sortByAscendingOrder_addIndex() {
        JavaRDD<Double> dimensionValues = fullBitmapStructureMock.getDimensionValues();
        JavaPairRDD<Double, Long> expectedValues = fullBitmapStructureMock.getValuesIndexed();

        JavaPairRDD<Double, Long> actualValues = null;//TODO bitmapStructure.getDistinctSortedWithIndex(dimensionValues);

        assertTrue(Rdds.areEqual(expectedValues, actualValues));
    }

    @Test
    public void calculateBitSets() {
        Long sizeOfUniqueValues = fullBitmapStructureMock.getSizeOfUniqueValues();
        JavaRDD<Double> dimensionValues = fullBitmapStructureMock.getDimensionValues();
        JavaPairRDD<Double, Long> currentData = fullBitmapStructureMock.getValuesIndexed();
        JavaRDD<BitSet> expectedBitSets = fullBitmapStructureMock.getValuesBitSets();

        JavaRDD<BitSet> actualBitSets = bitmapStructure.calculateBitSets(dimensionValues, currentData, sizeOfUniqueValues);

        assertTrue(Rdds.areEqual(expectedBitSets, actualBitSets));
    }

    @Test
    public void calculateBitSlices() {
        Long sizeOfUniqueValues = fullBitmapStructureMock.getSizeOfUniqueValues();
        JavaPairRDD<Double, Long> currentIndexed = fullBitmapStructureMock.getValuesIndexed();
        JavaRDD<BitSet> currentBitSets = fullBitmapStructureMock.getValuesBitSets();
        JavaPairRDD<Long, BitSet> expectedBitSlices = fullBitmapStructureMock.getValuesBitSlices();

        JavaPairRDD<Long, BitSet> actualBitSlices = bitmapStructure.calculateBitSlices(currentIndexed, currentBitSets, sizeOfUniqueValues);

        assertTrue(Rdds.areEqual(expectedBitSlices, actualBitSlices));
    }
}