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

public class BitmapStructureImplTest extends BaseSparkTest {

    private BitmapStructureImpl bitmapStructure;
    private FullBitmapStructureMock fullBitmapStructureMock;

    @Before
    public void setUp() {
        this.fullBitmapStructureMock = new FullDimensionXBitmapStructureMock(getSparkContextWrapper());
        BitSliceCreator bitSliceCreator = new BitSliceCreatorImpl();
        this.bitmapStructure = (BitmapStructureImpl) Injector.getBitmapStructure(getSparkContextWrapper(), 4);
    }


    @Test
    public void keepDistincts_sortByAscendingOrder_addIndex() {
        JavaRDD<Double> dimensionValues = fullBitmapStructureMock.getDimensionValues();
        JavaPairRDD<Double, Long> expectedValues = fullBitmapStructureMock.getValuesIndexed();

        JavaPairRDD<Double, Long> actualValues = bitmapStructure.getDistinctSortedWithIndex(dimensionValues);

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