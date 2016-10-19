package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions;
import org.junit.Before;
import org.junit.Test;

public class FlagProducerTest {
    private FlagProducer flagProducer;

    @Before public void beforeEach() {
        Point median = new Point(5, 5);
        flagProducer = new FlagProducer(median);
    }

    @Test public void point_withDimensionsSmallerThanMedian_producesFlag00() {
        Point point = new Point(4, 4);
        Flag expectedFlag = new Flag(false, false);

        assertFlagIsAsExpected(point, expectedFlag);
    }

    @Test public void point_withDimensionsLargerThanMedian_producesFlag11() {
        Point point = new Point(6, 6);
        Flag expectedFlag = new Flag(true, true);

        assertFlagIsAsExpected(point, expectedFlag);
    }

    @Test public void point_withDimensionsEqualToMedian_producesFlag11() {
        Point point = new Point(5, 5);
        Flag expectedFlag = new Flag(true, true);

        assertFlagIsAsExpected(point, expectedFlag);
    }

    @Test public void point_withFirstDimensionLargerAndSecondSmaller_producesFlag10() {
        Point point = new Point(6, 4);
        Flag expectedFlag = new Flag(true, false);

        assertFlagIsAsExpected(point, expectedFlag);
    }

    @Test public void point_withFirstDimensionSmallerAndSecondLarger_producesFlag01() {
        Point point = new Point(4, 6);
        Flag expectedFlag = new Flag(false, true);

        assertFlagIsAsExpected(point, expectedFlag);
    }

    private void assertFlagIsAsExpected(Point point, Flag expectedFlag) {
        Flag actualFlag = flagProducer.calculateFlag(point);

        MoreAssertions.assertThat(actualFlag).isEqualTo(expectedFlag);
    }
}