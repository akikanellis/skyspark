package com.github.dkanellis.skyspark.api.math.point;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import com.github.dkanellis.skyspark.api.math.point.PointUtils;
import org.apache.spark.api.java.JavaRDD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import suites.basic.BasicTestSuite;
import testcategories.BasicTest;

/**
 *
 * @author Dimitris Kanellis
 */
@Category(BasicTest.class)
public class PointUtilsTest {

    private static SparkContextWrapper sparkContext;

    @BeforeClass
    public static void setUpClass() {
        if (BasicTestSuite.sparkContext == null) {
            sparkContext = new SparkContextWrapper("PointUtilsTest", "local");
        } else {
            sparkContext = BasicTestSuite.sparkContext;
        }
    }

    @Test
    public void firstShouldDominateSecond() {
        System.out.println("dominates");
        Point2D first = new Point2D.Double(2080.877494624074, 2302.0770958188664);
        Point2D second = new Point2D.Double(5756.202069941658, 4418.941667115589);
        boolean result = PointUtils.dominates(first, second);
        assertTrue(result);
    }

    @Test
    public void firstShouldNotDominateSecond() {
        System.out.println("dominates");
        Point2D first = new Point2D.Double(6803.314583926934, 2266.355737840431);
        Point2D second = new Point2D.Double(5756.202069941658, 4418.941667115589);
        boolean result = PointUtils.dominates(first, second);
        assertFalse(result);
    }

    /**
     * Test of pointFromTextLine method, of class PointUtils.
     */
    @Test
    public void shouldReturnSamePoint2D() {
        System.out.println("pointFromTextLine");
        String textLine = " 6803.314583926934 2266.355737840431";
        String delimiter = " ";
        Point2D expResult = new Point2D.Double(6803.314583926934, 2266.355737840431);
        Point2D result = PointUtils.pointFromTextLine(textLine, delimiter);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetMedianPointFromRDD() {
        System.out.println("getMedianPointFromRDD");
        JavaRDD<Point2D> pointRDD = sparkContext.parallelize(getPoint2DList());

        Point2D expResult = new Point2D.Double(3401.657291963467, 2209.4708335577943);
        Point2D result = PointUtils.getMedianPointFromRDD(pointRDD);
        assertEquals(expResult, result);
    }

    private List<Point2D> getPoint2DList() {
        List<Point2D> points = new ArrayList<>();
        points.add(new Point2D.Double(2080.877494624074, 2302.0770958188664));
        points.add(new Point2D.Double(5756.202069941658, 4418.941667115589));
        points.add(new Point2D.Double(6803.314583926934, 2266.355737840431));
        points.add(new Point2D.Double(4610.505826490165, 3570.466435170513));
        points.add(new Point2D.Double(3475.4615053558455, 3488.0557122269856));

        return points;
    }
}
