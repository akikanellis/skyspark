package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public class Injector {
    public static BitmapStructure getBitmapStructure(JavaSparkContext sparkContext, final int numberOfPartitions) {
        JavaPairRDD<Long, BitSet> defaultValue = getDefaultValueRdd(sparkContext);
        BitSliceCreator bitSliceCreator = getBitSliceCreator();
        return new BitmapStructureImpl(numberOfPartitions, bitSliceCreator, defaultValue);
    }

    private static JavaPairRDD<Long, BitSet> getDefaultValueRdd(JavaSparkContext sparkContextWrapper) {
        Tuple2<Long, BitSet> defaultValue = BitSliceCreator.DEFAULT;
        List<Tuple2<Long, BitSet>> edgeCaseList = Collections.singletonList(defaultValue);
        JavaRDD<Tuple2<Long, BitSet>> rdd = sparkContextWrapper.parallelize(edgeCaseList);

        return rdd.mapToPair(p -> new Tuple2<>(p._1(), p._2()));
    }

    private static BitSliceCreator getBitSliceCreator() {
        return new BitSliceCreatorImpl();
    }
}
