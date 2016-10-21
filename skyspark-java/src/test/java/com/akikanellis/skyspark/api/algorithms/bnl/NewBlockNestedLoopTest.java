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

import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThatExceptionOfType;
import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThatP;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NewBlockNestedLoopTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    @Mock private Divider divider;
    @Mock private Merger merger;
    private NewBlockNestedLoop bnl;

    @Before public void beforeEach() { bnl = new NewBlockNestedLoop(divider, merger); }

    @Test public void emptyRdd_throwsIllegalArgumentException() {
        JavaRDD<Point> empty = sc.get().emptyRDD();

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> bnl.computeSkylinePoints(empty));
    }

    @Test public void populatedRdd_returnsSkylines() {
        JavaRDD<Point> points = sc.parallelize(new Point(5.9, 4.6), new Point(5.0, 4.1), new Point(3.6, 9.0));
        JavaPairRDD<Flag, Point> localSkylinesWithFlags = sc.parallelizePairs(
                new Tuple2<>(new Flag(true, true), new Point(5.9, 4.6)),
                new Tuple2<>(new Flag(true, true), new Point(5.0, 4.1)),
                new Tuple2<>(new Flag(true, true), new Point(3.6, 9.0))
        );
        JavaRDD<Point> expectedSkylines = sc.parallelize(new Point(5.0, 4.1), new Point(3.6, 9.0));
        when(divider.divide(points)).thenReturn(localSkylinesWithFlags);
        when(merger.merge(localSkylinesWithFlags)).thenReturn(expectedSkylines);

        JavaRDD<Point> skylines = bnl.computeSkylinePoints(points);

        assertThatP(skylines).containsOnlyElementsOf(expectedSkylines);
    }
}