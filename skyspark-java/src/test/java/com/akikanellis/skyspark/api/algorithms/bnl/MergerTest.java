package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.akikanellis.skyspark.api.test_utils.SparkContextRule;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import scala.Tuple2;

import java.util.List;

import static com.akikanellis.skyspark.api.test_utils.Mockitos.mockForSpark;
import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThatP;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MergerTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    private BnlAlgorithm bnlAlgorithm;
    private Merger merger;

    @Before public void beforeEach() {
        bnlAlgorithm = mockForSpark(BnlAlgorithm.class);
        merger = new Merger(bnlAlgorithm);
    }

    @Test public void mergingLocalSkylines_returnsGlobalSkylines() {
        List<Tuple2<Flag, Point>> flagsWithPointsList = asList(
                new Tuple2<>(new Flag(true, true), new Point(5.9, 4.6)),
                new Tuple2<>(new Flag(true, false), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, false), new Point(5.9, 4.0)),
                new Tuple2<>(new Flag(true, false), new Point(6.7, 3.3)),
                new Tuple2<>(new Flag(true, false), new Point(6.1, 3.4)),
                new Tuple2<>(new Flag(false, true), new Point(2.5, 7.3))
        );
        JavaPairRDD<Flag, Point> flagsWithPoints = sc.parallelizePairs(flagsWithPointsList);
        List<Point> expectedSkylinesList = asList(
                new Point(5.0, 4.1), new Point(5.9, 4.0),
                new Point(2.5, 7.3), new Point(6.7, 3.3), new Point(6.1, 3.4)
        );
        JavaRDD<Point> expectedSkylines = sc.parallelize(expectedSkylinesList);
        when(bnlAlgorithm.computeSkylinesWithPreComparison(flagsWithPointsList)).thenReturn(expectedSkylinesList);

        JavaRDD<Point> actualSkylines = merger.merge(flagsWithPoints);

        assertThatP(actualSkylines).containsOnlyElementsOf(expectedSkylines);
    }
}