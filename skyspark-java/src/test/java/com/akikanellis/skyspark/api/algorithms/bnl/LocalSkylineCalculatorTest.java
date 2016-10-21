package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.akikanellis.skyspark.api.test_utils.SparkContextRule;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

import static com.akikanellis.skyspark.api.test_utils.Mockitos.mockForSpark;
import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThat;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.when;

public class LocalSkylineCalculatorTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    private BnlAlgorithm bnlAlgorithm;
    private LocalSkylineCalculator localSkylineCalculator;

    @Before public void beforeEach() {
        bnlAlgorithm = mockForSpark(BnlAlgorithm.class);
        localSkylineCalculator = new LocalSkylineCalculator(bnlAlgorithm);
    }

    @Test public void producingLocalSkylines_producesSkylinesPerKey() {
        List<Tuple2<Flag, Point>> flagPointsList = asList(
                new Tuple2<>(new Flag(true, true), new Point(5.9, 4.6)),
                new Tuple2<>(new Flag(true, true), new Point(6.9, 5.6)),
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, false), new Point(5.9, 4.0))
        );
        whenAPointGroupIsToBeComputedReturnSkylines();
        JavaPairRDD<Flag, Point> flagPoints = sc.parallelizePairs(flagPointsList);
        List<Tuple2<Flag, Point>> expectedLocalSkylinesWithFlags = asList(
                new Tuple2<>(new Flag(true, true), new Point(5.9, 4.6)),
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, false), new Point(5.9, 4.0))
        );

        List<Tuple2<Flag, Point>> localSkylinesWithFlags
                = localSkylineCalculator.computeLocalSkylines(flagPoints).collect();

        assertThat(localSkylinesWithFlags).containsOnlyElementsOf(expectedLocalSkylinesWithFlags);
    }

    private void whenAPointGroupIsToBeComputedReturnSkylines() {
        when(bnlAlgorithm.computeSkylinesWithoutPreComparison(asList(new Point(5.9, 4.6), new Point(6.9, 5.6))))
                .thenReturn(singletonList(new Point(5.9, 4.6)));
        when(bnlAlgorithm.computeSkylinesWithoutPreComparison(asList(new Point(5.0, 4.1), new Point(5.9, 4.0))))
                .thenReturn(asList(new Point(5.0, 4.1), new Point(5.9, 4.0)));
    }

}