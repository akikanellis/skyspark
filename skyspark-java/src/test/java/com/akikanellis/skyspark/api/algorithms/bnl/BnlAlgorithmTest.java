package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThat;

public class BnlAlgorithmTest {
    private BnlAlgorithm bnl;

    @Before public void beforeEach() { bnl = new BnlAlgorithm(); }

    @Test public void computingSkylinesWithoutPreComparison_withPoints_returnsCorrectSkylines() {
        List<Point> points = Arrays.asList(
                new Point(5.4, 4.4), new Point(5.0, 4.1), new Point(3.6, 9.0),
                new Point(5.9, 4.0), new Point(5.9, 4.6), new Point(2.5, 7.3),
                new Point(6.3, 3.5), new Point(9.9, 4.1), new Point(6.7, 3.3), new Point(6.1, 3.4));
        List<Point> expectedSkylines = Arrays.asList(
                new Point(5.0, 4.1), new Point(5.9, 4.0),
                new Point(2.5, 7.3), new Point(6.7, 3.3), new Point(6.1, 3.4));

        Iterable<Point> actualSkylines = bnl.computeSkylinesWithoutPreComparison(points);

        assertThat(actualSkylines).containsOnlyElementsOf(expectedSkylines);
    }

    @Test public void computingSkylinesWithoutPreComparison_withOnlySkylines_returnsTheSamePoints() {
        List<Point> points = Arrays.asList(
                new Point(5.0, 4.1), new Point(5.9, 4.0), new Point(2.5, 7.3),
                new Point(6.7, 3.3), new Point(6.1, 3.4)
        );

        Iterable<Point> actualSkylines = bnl.computeSkylinesWithoutPreComparison(points);

        assertThat(actualSkylines).containsOnlyElementsOf(points);
    }

    @Test public void computingSkylinesWithPreComparison_withPoints_returnsCorrectSkylines() {
        List<Tuple2<Flag, Point>> flagsPoints = Arrays.asList(
                new Tuple2<>(new Flag(true, true), new Point(5.9, 4.6)),
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, false), new Point(5.9, 4.0)),
                new Tuple2<>(new Flag(true, false), new Point(6.7, 3.3)),
                new Tuple2<>(new Flag(true, false), new Point(6.1, 3.4)),
                new Tuple2<>(new Flag(false, true), new Point(2.5, 7.3)));
        List<Point> expectedSkylines = Arrays.asList(
                new Point(5.0, 4.1), new Point(5.9, 4.0),
                new Point(2.5, 7.3), new Point(6.7, 3.3), new Point(6.1, 3.4));

        Iterable<Point> actualSkylines = bnl.computeSkylinesWithPreComparison(flagsPoints);

        assertThat(actualSkylines).containsOnlyElementsOf(expectedSkylines);
    }

    @Test public void computingSkylinesWithtPreComparison_withOnlySkylines_returnsTheSamePoints() {
        List<Tuple2<Flag, Point>> flagsPoints = Arrays.asList(
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, false), new Point(5.9, 4.0)),
                new Tuple2<>(new Flag(true, false), new Point(6.7, 3.3)),
                new Tuple2<>(new Flag(true, false), new Point(6.1, 3.4)),
                new Tuple2<>(new Flag(false, true), new Point(2.5, 7.3)));
        List<Point> expectedSkylines = Arrays.asList(
                new Point(5.0, 4.1), new Point(5.9, 4.0), new Point(6.7, 3.3),
                new Point(2.5, 7.3), new Point(6.1, 3.4));

        Iterable<Point> actualSkylines = bnl.computeSkylinesWithPreComparison(flagsPoints);

        assertThat(actualSkylines).containsOnlyElementsOf(expectedSkylines);
    }
}