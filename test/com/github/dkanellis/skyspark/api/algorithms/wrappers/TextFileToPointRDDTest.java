package com.github.dkanellis.skyspark.api.algorithms.wrappers;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import suites.BasicTestSuite;
import testcategories.BasicTest;

/**
 *
 * @author Dimitris Kanellis
 */
@Category(BasicTest.class)
public class TextFileToPointRDDTest {

    private static SparkContextWrapper sparkContext;

    public TextFileToPointRDDTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        if (BasicTestSuite.sparkContext == null) {
            sparkContext = new SparkContextWrapper("TextFileToPointRDDTest", "local");
        } else {
            sparkContext = BasicTestSuite.sparkContext;
        }
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Test
    public void testGetPointRDDFromTextFile() {
        System.out.println("getPointRDDFromTextFile");
        String filePath = "data/datasets/UNIFORM_2_10.txt";
        String delimiter = " ";
        TextFileToPointRDD instance = new TextFileToPointRDD(sparkContext);
        
        JavaRDD<Point2D> expResult = getExpectedRDD();
        JavaRDD<Point2D> result = instance.getPointRDDFromTextFile(filePath, delimiter);
        
        assertEquals(expResult.collect(), result.collect());
    }

    private JavaRDD<Point2D> getExpectedRDD() {
        List<Point2D> points = getPointList();
        JavaRDD<Point2D> pointsRDD = sparkContext.parallelize(points);
        return pointsRDD;
    }

    private List<Point2D> getPointList() {
        List<Point2D> points = new ArrayList<>();
        points.add(new Point2D.Double(7384.475430902016, 6753.440980966086));
        points.add(new Point2D.Double(5132.274536706283, 176.07686372999513));
        points.add(new Point2D.Double(4691.353681108572, 8232.92056883113));
        points.add(new Point2D.Double(6292.759807632455, 3790.860474769524));
        points.add(new Point2D.Double(7250.357537607312, 2525.5019336970545));
        points.add(new Point2D.Double(755.6005425518632, 9555.368016130034));
        points.add(new Point2D.Double(4901.088081890441, 1031.5393572675114));
        points.add(new Point2D.Double(1963.7806084781564, 5979.674156591443));
        points.add(new Point2D.Double(6676.451493576449, 3373.7938261245263));
        points.add(new Point2D.Double(2604.9875068505676, 8312.417734885097));
        return points;
    }

}
