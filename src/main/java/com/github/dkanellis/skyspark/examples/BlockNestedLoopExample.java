package com.github.dkanellis.skyspark.examples;

import com.github.dkanellis.skyspark.api.algorithms.factories.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class BlockNestedLoopExample {

    public static void main(String[] args) {
        SparkContextWrapper sparkContext = new SparkContextWrapper("Block-Nested Loop example", "local[4]");
        SkylineAlgorithmFactory algorithmFactory = new SkylineAlgorithmFactory();
        SkylineAlgorithm blockNestedLoop = algorithmFactory.getBlockNestedLoop(sparkContext);

        List<Point2D> skylines = blockNestedLoop.getSkylinePoints(Datasets.UNIFORM_SMALL);
        Datasets.print(skylines);
    }
}
