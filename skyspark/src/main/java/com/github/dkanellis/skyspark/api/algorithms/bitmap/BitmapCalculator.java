package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.utils.BitSets;
import com.github.dkanellis.skyspark.api.utils.point.DominationComparatorMinAnotation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;


class BitmapCalculator implements Serializable {

    private final int numberOfPartitions;
    private final BitSliceCreator bitSliceCreator;
    private final JavaPairRDD<Long, BitSet> defaultValueRdd;

    public BitmapCalculator(final int numberOfPartitions, JavaSparkContext sparkContext) {
        this.numberOfPartitions = numberOfPartitions;
        this.defaultValueRdd = getDefaultValueRdd(sparkContext);
        this.bitSliceCreator = new BitSliceCreatorImpl();
    }

    static Iterable<Tuple2<Integer, Double>> split(Point2D p) {
        List<Tuple2<Integer, Double>> splitByDimension = new ArrayList<>();
        splitByDimension.add(new Tuple2<>(1, p.getX()));
        splitByDimension.add(new Tuple2<>(2, p.getY()));
        return splitByDimension;
    }

    private JavaPairRDD<Long, BitSet> getDefaultValueRdd(JavaSparkContext sparkContextWrapper) {
        Tuple2<Long, BitSet> defaultValue = BitSliceCreator.DEFAULT;
        List<Tuple2<Long, BitSet>> edgeCaseList = Collections.singletonList(defaultValue);
        JavaRDD<Tuple2<Long, BitSet>> rdd = sparkContextWrapper.parallelize(edgeCaseList);

        return rdd.mapToPair(p -> new Tuple2<>(p._1(), p._2()));
    }

    public JavaRDD<Map<Tuple2<Integer, Integer>, BitSet>> computeBitSlices(@NotNull JavaRDD<Point2D> points) {
        checkNotNull(points);

        JavaPairRDD<Integer, Double> dimensionsWithValues = points.flatMapToPair(BitmapCalculator::split);

        JavaPairRDD<Integer, Iterable<Double>> grouped = dimensionsWithValues.groupByKey();

        JavaPairRDD<Tuple2<Integer, Double>, Integer> rankedd;

        JavaRDD<Map<Tuple2<Integer, Integer>, BitSet>> bitmaps = grouped.map(p -> {
            final int currentDimension = p._1();
            Iterable<Double> allDimensionValues = p._2();

            SortedSet<Double> distinctSorted = new TreeSet<>(new DominationComparatorMinAnotation());
            allDimensionValues.forEach(distinctSorted::add);
            final int totalRanks = distinctSorted.size();

            Map<Double, Integer> ranked = new HashMap<>();
            int index = 0;
            for (Double distinctValue : distinctSorted) {
                ranked.put(distinctValue, index++);
            }

            List<BitSet> bitSets = new ArrayList<>();
            allDimensionValues.forEach(v -> bitSets.add(BitSets.bitSetFromIndexes(0, totalRanks - ranked.get(v))));
            final int totalNumberOfElements = bitSets.size();
            ////////////////////////////////////////////////////

            Map<Tuple2<Integer, Integer>, BitSet> finalMap = new HashMap<>();
            for (int rank = 0; rank < totalRanks; ++rank) {
                Tuple2<Integer, Integer> key = new Tuple2<>(currentDimension, rank);
                BitSet bitSlice = new BitSet(totalNumberOfElements);
                for (int currentElement = 0; currentElement < totalNumberOfElements; ++currentElement) {
                    BitSet currentBitSet = bitSets.get(currentElement);
                    final boolean correspondingBit = currentBitSet.get(totalRanks - rank - 1);
                    final int correspondingBitIndex = totalNumberOfElements - currentElement - 1;
                    bitSlice.set(correspondingBitIndex, correspondingBit);
                }

                finalMap.put(key, bitSlice);
            }

            return finalMap;
        });

        bitmaps.count();


        return bitmaps;
    }

    JavaRDD<BitSet> calculateBitSets(JavaRDD<Double> dimensionValues, JavaPairRDD<Double, Long> indexed, Long sizeOfUniqueValues) {
        return dimensionValues
                .zipWithIndex()
                .join(indexed)
                .mapToPair(v -> new Tuple2<>(BitSets.bitSetFromIndexes(0, sizeOfUniqueValues - v._2()._2()), v._2()._1()))
                .map(Tuple2::swap)
                .sortBy(Tuple2::_1, true, numberOfPartitions)
                .map(Tuple2::_2);
    }

    JavaPairRDD<Long, BitSet> calculateBitSlices(JavaPairRDD<Double, Long> indexed, JavaRDD<BitSet> bitSets, Long sizeOfUniqueValues) {
        JavaRDD<List<BitSet>> glomed = bitSets
                .coalesce(1)
                .glom();

        return glomed
                .cartesian(indexed)
                .mapToPair(p -> bitSliceCreator.from(p, sizeOfUniqueValues))
                .union(defaultValueRdd);
    }
}