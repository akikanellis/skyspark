#SkySpark

SkySpark is a library for solving the skyline query fast and efficiently on a cluster environment using the Apache Spark framework. The API includes different algorithms so the user can choose the best one for his particular environment and is also very simple to use. Each algorithm has been fine-tuned and configured to perform best on the Apache Spark framework.

##Algorithms

The algorithms that can be used are:

* Block-Nested Loop
* Sort Filter Skyline
* Bitmap

##Modules
###Performance module

####Usage
You can view the help menu of the jar like so:

`java -jar performance-1.0.jar --help`

The available options are:

```
Usage: SkySpark performance testing [options]
  Options:
    --help
       
       Default: false
  * -a, -algorithm
       The algorithm(s) to use
  * -f, -file
       The filepath(s) of the points
    -o, -output
       The output file with the results, can only be .txt or .xls
       Default: Results of 2016-01-03--18-08-14.xls
    -s, -slaves
       The number of slaves used
       Default: 0
    -t, -times
       How many times to run per algorithm and file combination
       Default: 1
```
####Study
The *performance* module serves as a cluster-ready jar in case you want to test out the algorithms and their performance in a cluster environment. There have already been done ~2500 experiments in different cluster sizes, memory sizes, data types and data sizes for the algorithms which proved the algorithms to be very fast in the Apache Spark framework.

###Examples module
In the [examples module](examples) you can find an example dataset as well as the usage of the library

##Usage
The usage is as simple as this:

```Java
JavaRDD<Point2D> pointRdd = ;// get your point RDD here
SkylineAlgorithm skylineAlgorithm = new BlockNestedLoop();
JavaRDD<Point2D> skylines = skylineAlgorithm.computeSkylinePoints(pointRdd);
```

##Third-party libraries
 * [Apache Spark 2.10-1.3.1](https://spark.apache.org/)

##License
This application itself is released under **MIT** license, see [LICENSE](./LICENSE).

All 3rd party open sourced libs distributed with this application are still under their own license.
