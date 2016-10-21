package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.akikanellis.skyspark.api.test_utils.SparkContextRule;
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

import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThatP;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DividerTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    @Mock private FlagAdder flagAdder;
    @Mock private LocalSkylineCalculator localSkylineCalculator;
    private Divider divider;

    @Before public void beforeEach() { divider = new Divider(flagAdder, localSkylineCalculator); }

    @Test public void producingLocalSkylines_returnsCorrectSkylines() {
        JavaRDD<Point> points = sc.parallelize(Arrays.asList(
                new Point(5.9, 4.6),
                new Point(6.9, 5.6),
                new Point(5.0, 4.1),
                new Point(5.9, 4.0))
        );
        JavaPairRDD<Flag, Point> flagPoints = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Flag(true, true), new Point(5.9, 4.6)),
                new Tuple2<>(new Flag(true, true), new Point(6.9, 5.6)),
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, false), new Point(5.9, 4.0)))
        );
        when(flagAdder.addFlags(points)).thenReturn(flagPoints);
        JavaPairRDD<Flag, Point> expectedLocalSkylinesWithFlags = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Flag(true, true), new Point(5.9, 4.6)),
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, false), new Point(5.9, 4.0)))
        );
        when(localSkylineCalculator.computeLocalSkylines(flagPoints)).thenReturn(expectedLocalSkylinesWithFlags);

        JavaPairRDD<Flag, Point> actualLocalSkylinesWithFlags = divider.divide(points);

        assertThatP(actualLocalSkylinesWithFlags).containsOnlyElementsOf(expectedLocalSkylinesWithFlags);
    }
}