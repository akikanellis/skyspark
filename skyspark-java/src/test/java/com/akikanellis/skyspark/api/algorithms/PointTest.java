package com.akikanellis.skyspark.api.algorithms;


import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Before;
import org.junit.Test;

import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThat;
import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThatExceptionOfType;


public class PointTest {
    private Point point;

    @Before public void beforeEach() { point = new Point(5, 2, 7, 1); }

    @Test public void equalsContract_isCorrect() { EqualsVerifier.forClass(Point.class).verify(); }

    @Test public void gettingDimension_withLessThanZeroIndex_throwsIndexOutOfBounds() {
        assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(() -> point.dimension(-1));
    }

    @Test public void gettingDimension_withBiggerThanSizeIndex_throwsIndexOutOfBounds() {
        assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(() -> point.dimension(4));
    }

    @Test public void gettingDimension_withZeroIndex_retrievesFirstDimension() {
        assertThat(point.dimension(0)).isEqualTo(5);
    }

    @Test public void gettingDimension_withThreeIndex_retrievesLastDimension() {
        assertThat(point.dimension(3)).isEqualTo(1);
    }
}