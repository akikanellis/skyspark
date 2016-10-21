package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.akikanellis.skyspark.api.test_utils.SparkContextRule;
import com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static com.akikanellis.skyspark.api.test_utils.Mockitos.mockForSpark;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlagAdderTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    @Mock private MedianFinder medianFinder;
    private FlagProducer flagProducer;
    private FlagAdder flagAdder;

    @Before public void beforeEach() {
        flagProducer = mockForSpark(FlagProducer.class);
        flagAdder = new FlagAdder(medianFinder) {
            @Override protected FlagProducer createFlagProducer(Point median) { return flagProducer; }
        };
    }

    @Test public void setOfPoints_returnsThemWithTheirFlags() {
        JavaRDD<Point> points = sc.parallelize(new Point(5.4, 4.4), new Point(5.0, 4.1), new Point(3.6, 9.0));
        List<Tuple2<Flag, Point>> expectedFlagPointsList = Arrays.asList(
                new Tuple2<>(new Flag(true, false), new Point(5.4, 4.4)),
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, true), new Point(3.6, 9.0))
        );
        JavaPairRDD<Flag, Point> expectedFlagPoints = sc.parallelizePairs(expectedFlagPointsList);
        when(medianFinder.getMedian(points)).thenReturn(new Point(2.7, 4.5));
        whenFlagIsRequestedForEachPointReturnIt(expectedFlagPointsList);

        JavaPairRDD<Flag, Point> actualFlagPoints = flagAdder.addFlags(points);

        MoreAssertions.assertThatP(actualFlagPoints).containsOnlyElementsOf(expectedFlagPoints);
    }

    private void whenFlagIsRequestedForEachPointReturnIt(List<Tuple2<Flag, Point>> expectedFlagPointsList) {
        expectedFlagPointsList.forEach(fp -> when(flagProducer.calculateFlag(fp._2())).thenReturn(fp._1()));
    }
}