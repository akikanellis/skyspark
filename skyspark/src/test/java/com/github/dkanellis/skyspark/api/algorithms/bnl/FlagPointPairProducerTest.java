package com.github.dkanellis.skyspark.api.algorithms.bnl;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Tuple2;
import com.github.dkanellis.skyspark.api.testUtils.categories.types.UnitTests;

import java.awt.geom.Point2D;

import static org.junit.Assert.assertEquals;

@Category(UnitTests.class)
public class FlagPointPairProducerTest {

    private FlagPointPairProducer producer;

    @Before
    public void setUp() {
        Point2D medianPoint = new Point2D.Double(5, 5);
        producer = new FlagPointPairProducer(medianPoint);
    }

    @Test
    public void shouldReturnSamePair() {
        PointFlag expectedFlag = new PointFlag(1, 1);
        Point2D expectedPoint = new Point2D.Double(4610.505826490165, 3570.466435170513);
        Tuple2<PointFlag, Point2D> expectedResult = new Tuple2<>(expectedFlag, expectedPoint);

        Tuple2<PointFlag, Point2D> actualResult = producer.getFlagPointPair(expectedPoint);

        assertEquals(expectedResult, actualResult);
    }
}
