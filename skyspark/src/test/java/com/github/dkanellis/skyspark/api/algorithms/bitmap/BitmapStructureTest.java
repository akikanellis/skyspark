package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.test_utils.Rdds;
import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertTrue;

public class BitmapStructureTest extends BaseSparkTest {

    private BitmapStructure bitmapStructure;

    @Before
    public void setUp() {
        this.bitmapStructure = new BitmapStructure(get10Points(), 4);
    }


    @Test
    public void keepDistinctsOfXDimensions_andSortByAscendingOrder() {
        JavaRDD<Point2D> currentPointsRdd = get10Points();
        JavaRDD<Double> expectedValues = getDistinctXValuesSorted();

        JavaRDD<Double> actualValues = bitmapStructure.getDistinctSorted(currentPointsRdd, 1);

        assertTrue(Rdds.areEqual(expectedValues, actualValues));
    }

    @Test
    public void keepDistinctsOfYDimension_andSortByAscendingOrder() {
        JavaRDD<Point2D> currentPointsRdd = get10Points();
        JavaRDD<Double> expectedValues = getDistinctYValuesSorted();

        JavaRDD<Double> actualValues = bitmapStructure.getDistinctSorted(currentPointsRdd, 2);

        assertTrue(Rdds.areEqual(expectedValues, actualValues));
    }

    @Test
    public void xValuesmappedByIndex() {
        JavaRDD<Double> currentPoints = getDistinctXValuesSorted();
        JavaPairRDD<Long, Double> expectedPoints = getXValuesIndexed();

        JavaPairRDD<Long, Double> actualPoints = bitmapStructure.mapWithIndex(currentPoints);

        assertTrue(Rdds.areEqual(expectedPoints, actualPoints));
    }

    @Test
    public void yValuesmappedByIndex() {
        JavaRDD<Double> currentPoints = getDistinctYValuesSorted();
        JavaPairRDD<Long, Double> expectedPoints = getYValuesIndexed();

        JavaPairRDD<Long, Double> actualPoints = bitmapStructure.mapWithIndex(currentPoints);

        assertTrue(Rdds.areEqual(expectedPoints, actualPoints));
    }

    private JavaRDD<Point2D> get10Points() {
        return toRdd(Arrays.asList(
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
        ));
    }

    private List<Point2D> get10PointsSkylines() {
        return Arrays.asList(
                new Point2D.Double(5.0, 4.1),
                new Point2D.Double(5.9, 4.0),
                new Point2D.Double(6.7, 3.3),
                new Point2D.Double(6.1, 3.4),
                new Point2D.Double(2.5, 7.3));
    }

    private JavaRDD<Double> getDistinctXValuesSorted() {
        return toRdd(Arrays.asList(2.5, 3.6, 5.0, 5.4, 5.9, 6.1, 6.3, 6.7, 9.9));
    }

    private JavaRDD<Double> getDistinctYValuesSorted() {
        return toRdd(Arrays.asList(3.3, 3.4, 3.5, 4.0, 4.1, 4.4, 4.6, 7.3, 9.0));
    }

    private JavaPairRDD<Long, Double> getXValuesIndexed() {
        return toPairRdd(Arrays.asList(
                new Tuple2<>(0L, 2.5),
                new Tuple2<>(1L, 3.6),
                new Tuple2<>(2L, 5.0),
                new Tuple2<>(3L, 5.4),
                new Tuple2<>(4L, 5.9),
                new Tuple2<>(5L, 6.1),
                new Tuple2<>(6L, 6.3),
                new Tuple2<>(7L, 6.7),
                new Tuple2<>(8L, 9.9)
        ));
    }

    private JavaPairRDD<Long, Double> getYValuesIndexed() {
        return toPairRdd(Arrays.asList(
                new Tuple2<>(0L, 3.3),
                new Tuple2<>(1L, 3.4),
                new Tuple2<>(2L, 3.5),
                new Tuple2<>(3L, 4.0),
                new Tuple2<>(4L, 4.1),
                new Tuple2<>(5L, 4.4),
                new Tuple2<>(6L, 4.6),
                new Tuple2<>(7L, 7.3),
                new Tuple2<>(8L, 9.0)
        ));
    }
}