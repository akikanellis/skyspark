package com.akikanellis.skyspark.api.test_utils.assertions;

import com.akikanellis.skyspark.api.algorithms.bnl.PointFlag;
import org.apache.spark.api.java.JavaPairRDD;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.List;

public class FlagPointRddAssert extends AbstractAssert<FlagPointRddAssert, JavaPairRDD<PointFlag, Point2D>> {

    FlagPointRddAssert(JavaPairRDD<PointFlag, Point2D> actual) { super(actual, FlagPointRddAssert.class); }

    static FlagPointRddAssert assertThat(JavaPairRDD<PointFlag, Point2D> actual) {
        return new FlagPointRddAssert(actual);
    }

    /**
     * Verifies that the actual RDD contains only the given values and nothing else, <b>in any order</b>.
     *
     * @param values the given values.
     * @return {@code this} assertion object.
     * @throws NullPointerException     if the given argument is {@code null}.
     * @throws IllegalArgumentException if the given argument is an empty array.
     * @throws AssertionError           if the actual RDD is {@code null}.
     * @throws AssertionError           if the actual RDD does not contain the given values, i.e. the actual RDD
     *                                  contains some or none of the given values, or the actual RDD contains more
     *                                  values than the given ones.
     * @see org.assertj.core.api.AbstractIterableAssert#containsOnly(Object[])
     */
    public FlagPointRddAssert containsOnly(Tuple2<PointFlag, Point2D>... values) {
        isNotNull();

        List<Tuple2<PointFlag, Point2D>> actualList = actual.collect();

        Assertions.assertThat(actualList).containsOnly(values);

        return this;
    }

    /**
     * Same semantic as {@link #containsOnly(Tuple2[])} : verifies that actual contains all the elements of the given
     * RDD and nothing else, <b>in any order</b>.
     *
     * @param expected the given {@code JavaPairRDD<PointFlag, Point2D>} we will get elements from.
     * @see org.assertj.core.api.AbstractIterableAssert#containsOnlyElementsOf(Iterable)
     */
    public FlagPointRddAssert containsOnlyElementsOf(JavaPairRDD<PointFlag, Point2D> expected) {
        isNotNull();

        List<Tuple2<PointFlag, Point2D>> actualList = actual.collect();
        List<Tuple2<PointFlag, Point2D>> expectedList = expected.collect();

        Assertions.assertThat(actualList).containsOnlyElementsOf(expectedList);

        return this;
    }

    /**
     * Verifies that the actual RDD contains only the given values and nothing else, <b>in order</b>.<br>
     * This assertion should only be used with RDDs that have a consistent iteration order, prefer
     * {@link #containsOnly(Tuple2[])} in that case).
     *
     * @param values the given values.
     * @return {@code this} assertion object.
     * @throws NullPointerException if the given argument is {@code null}.
     * @throws AssertionError       if the actual RDD is {@code null}.
     * @throws AssertionError       if the actual RDD does not contain the given values with same order, i.e. the actual
     *                              RDD contains some or none of the given values, or the actual RDD contains more
     *                              values than the given ones or values are the same but the order is not.
     * @see org.assertj.core.api.AbstractIterableAssert#containsExactly(Object[])
     */
    public FlagPointRddAssert containsExactly(@SuppressWarnings("unchecked") Tuple2<PointFlag, Point2D>... values) {
        isNotNull();

        List<Tuple2<PointFlag, Point2D>> actualList = actual.collect();

        Assertions.assertThat(actualList).containsExactly(values);

        return this;
    }

    /**
     * Same as {@link #containsExactly(Tuple2[])} but handle the {@link Iterable} to array conversion : verifies that
     * actual contains all the elements of the given RDD and nothing else <b>in the same order</b>.
     *
     * @param expected the given {@code JavaPairRDD<PointFlag, Point2D>} we will get elements from.
     * @see org.assertj.core.api.AbstractIterableAssert#containsExactlyElementsOf(Iterable)
     */
    public FlagPointRddAssert containsExactlyElementsOf(JavaPairRDD<PointFlag, Point2D> expected) {
        isNotNull();

        List<Tuple2<PointFlag, Point2D>> actualList = actual.collect();
        List<Tuple2<PointFlag, Point2D>> expectedList = expected.collect();

        Assertions.assertThat(actualList).containsExactlyElementsOf(expectedList);

        return this;
    }
}
