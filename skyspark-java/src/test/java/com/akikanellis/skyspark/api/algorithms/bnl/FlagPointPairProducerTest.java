package com.akikanellis.skyspark.api.algorithms.bnl;

import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.awt.geom.Point2D;

import static org.assertj.core.api.Assertions.assertThat;

public class FlagPointPairProducerTest {

    private OlgFlagPointPairProducer producer;

    @Before
    public void setUp() {
        Point2D medianPoint = new Point2D.Double(5, 5);
        producer = new OlgFlagPointPairProducer(medianPoint);
    }

    @Test
    public void shouldReturnSamePair() {
        OldPointFlag expectedFlag = new OldPointFlag(1, 1);
        Point2D expectedPoint = new Point2D.Double(4610.505826490165, 3570.466435170513);
        Tuple2<OldPointFlag, Point2D> expectedResult = new Tuple2<>(expectedFlag, expectedPoint);

        Tuple2<OldPointFlag, Point2D> actualResult = producer.getFlagPointPair(expectedPoint);

        assertThat(actualResult).isEqualTo(expectedResult);
    }
}
