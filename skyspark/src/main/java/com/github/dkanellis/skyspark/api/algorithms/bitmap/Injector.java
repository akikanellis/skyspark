package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public class Injector {
    public static BitmapStructure getBitmapStructure(SparkContextWrapper sparkContextWrapper, final int numberOfPartitions) {
        JavaPairRDD<Long, BitSlice> defaultValue = getDefaultValueRdd(sparkContextWrapper);
        BitSliceCreator bitSliceCreator = getBitSliceCreator();
        return new BitmapStructureImpl(numberOfPartitions, bitSliceCreator, defaultValue);
    }

    private static JavaPairRDD<Long, BitSlice> getDefaultValueRdd(SparkContextWrapper sparkContextWrapper) {
        Tuple2<Long, BitSlice> defaultValue = new Tuple2<>(-1L, new BitSlice(-1L, 0, new BitSet(0)));
        List<Tuple2<Long, BitSlice>> edgeCaseList = Collections.singletonList(defaultValue);
        JavaRDD<Tuple2<Long, BitSlice>> rdd = sparkContextWrapper.parallelize(edgeCaseList);
        JavaPairRDD<Long, BitSlice> defaultValueRdd = rdd.mapToPair(p -> new Tuple2<Long, BitSlice>(p._1(), p._2()));
        return defaultValueRdd;
    }

    private static BitSliceCreator getBitSliceCreator() {
        return new BitSliceCreatorImpl();
    }
}
