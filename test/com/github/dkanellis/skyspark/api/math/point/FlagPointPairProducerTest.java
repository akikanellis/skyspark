package com.github.dkanellis.skyspark.api.math.point;

import com.github.dkanellis.skyspark.api.testcategories.SmallInputTest;
import java.awt.geom.Point2D;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.experimental.categories.Category;
import scala.Tuple2;

/**
 *
 * @author Dimitris Kanellis
 */
@Category(SmallInputTest.class)
public class FlagPointPairProducerTest {
    private static Point2D medianPoint;
    private static FlagPointPairProducer producer;
    
    @BeforeClass
    public static void setUpClass() {
        medianPoint = new Point2D.Double(5, 5);
        producer = new FlagPointPairProducer(medianPoint);
    }

    /**
     * Test of getFlagPointPair method, of class FlagPointPairProducer.
     */
    @Test
    public void shouldReturnSamePair() {
        System.out.println("getFlagPointPair");
        PointFlag expFlag = new PointFlag(1, 1);
        Point2D expPoint = new Point2D.Double(4610.505826490165, 3570.466435170513);
        
        Tuple2<PointFlag, Point2D> expResult = new Tuple2<>(expFlag, expPoint);
        Tuple2<PointFlag, Point2D> result = producer.getFlagPointPair(expPoint);
        assertEquals(expResult, result);
    }
    
}
