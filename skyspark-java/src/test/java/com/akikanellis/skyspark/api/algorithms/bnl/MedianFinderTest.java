package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.akikanellis.skyspark.api.test_utils.SparkContextRule;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThat;

public class MedianFinderTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    private MedianFinder medianFinder;

    @Before public void beforeEach() { medianFinder = new MedianFinder(); }

    @Test public void singlePoint_producesCorrectMedian() {
        JavaRDD<Point> points = sc.parallelize(new Point(1.0));
        Point expectedMedian = new Point(0.5);

        assertExpectedIsMedian(points, expectedMedian);
    }

    @Test public void oneDimensionalPoints_produceCorrectMedian() {
        JavaRDD<Point> points = sc.parallelize(
                new Point(5.2),
                new Point(9.5),
                new Point(4.6)
        );
        Point expectedMedian = new Point(4.75);

        assertExpectedIsMedian(points, expectedMedian);
    }

    @Test public void twoDimensionalPoints_produceCorrectMedian() {
        JavaRDD<Point> points = sc.parallelize(
                new Point(5.2, 2.1),
                new Point(9.5, 1.4),
                new Point(4.6, 4.4)
        );
        Point expectedMedian = new Point(4.75, 2.2);

        assertExpectedIsMedian(points, expectedMedian);
    }

    @Test public void threeDimensionalPoints_produceCorrectMedian() {
        JavaRDD<Point> points = sc.parallelize(
                new Point(5.2, 2.1, 6.7),
                new Point(9.5, 1.4, 7.3),
                new Point(4.6, 4.4, 5.6)
        );
        Point expectedMedian = new Point(4.75, 2.2, 3.65);

        assertExpectedIsMedian(points, expectedMedian);
    }

    private void assertExpectedIsMedian(JavaRDD<Point> points, Point expectedMedian) {
        Point actualMedian = medianFinder.getMedian(points);

        assertThat(actualMedian).isEqualTo(expectedMedian);
    }
}