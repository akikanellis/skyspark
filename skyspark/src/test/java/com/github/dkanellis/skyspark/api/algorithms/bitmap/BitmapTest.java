package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.test_utils.Rdds;
import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import junit.framework.Assert;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BitmapTest extends BaseSparkTest {

    private Bitmap bitmap;

    public static List<Point2D> get10PointsSkylines() {
        List<Point2D> points = new ArrayList<>();

        points.add(new Point2D.Double(5.0, 4.1));
        points.add(new Point2D.Double(5.9, 4.0));
        points.add(new Point2D.Double(6.7, 3.3));
        points.add(new Point2D.Double(6.1, 3.4));
        points.add(new Point2D.Double(2.5, 7.3));

        return points;
    }

    @Before
    public void setUp() {
        bitmap = new Bitmap(1);
    }

    @Test
    public void keepDistinctsOfXDimensions_andSortByAscendingOrder() {
        JavaRDD<Point2D> currentPointsRdd = toRdd(get10Points());
        JavaRDD<Double> expectedValues = toRdd(getDistinctXValuesSorted());

        JavaRDD<Double> actualValues = bitmap.getDistinctSorted(currentPointsRdd, 1);

        Assert.assertTrue(Rdds.areEqual(expectedValues, actualValues));
    }

    @Test
    public void keepDistinctsOfYDimension_andSortByAscendingOrder() {
        JavaRDD<Point2D> currentPointsRdd = toRdd(get10Points());
        JavaRDD<Double> expectedValues = toRdd(getDistinctYValuesSorted());

        JavaRDD<Double> actualValues = bitmap.getDistinctSorted(currentPointsRdd, 2);

        Assert.assertTrue(Rdds.areEqual(expectedValues, actualValues));
    }

    @Test
    @Ignore("To be implemented")
    public void forEachXofPoint_returnBitVector() {

    }

    @Test
    @Ignore("To be implemented")
    public void forEachYofPoint_returnBitVector() {
    }

    @Test
    public void toString_returnName() {
        String expectedName = "Bitmap";

        String actualName = bitmap.toString();

        assertEquals(expectedName, actualName);
    }

    private List<Point2D> get10Points() {
        List<Point2D> points = new ArrayList<>();

        points.add(new Point2D.Double(5.4, 4.4));
        points.add(new Point2D.Double(5.0, 4.1));
        points.add(new Point2D.Double(3.6, 9.0));
        points.add(new Point2D.Double(5.9, 4.0));
        points.add(new Point2D.Double(5.9, 4.6));
        points.add(new Point2D.Double(2.5, 7.3));
        points.add(new Point2D.Double(6.3, 3.5));
        points.add(new Point2D.Double(9.9, 4.1));
        points.add(new Point2D.Double(6.7, 3.3));
        points.add(new Point2D.Double(6.1, 3.4));

        return points;
    }

    private List<Double> getDistinctXValuesSorted() {
        return Arrays.asList(2.5, 3.6, 5.0, 5.4, 5.9, 6.1, 6.3, 6.7, 9.9);
    }

    private List<Double> getDistinctYValuesSorted() {
        return Arrays.asList(3.3, 3.4, 3.5, 4.0, 4.1, 4.4, 4.6, 7.3, 9.0);
    }
}