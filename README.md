SkySpark
========

SkySpark is a project for solving the skyline query fast and efficiently on a cluster environment using Apache Spark. The API includes different algorithms so the user can choose the best one for his particular environment and is also very simple to use. Each algorithm has been fine-tuned and configure to perform best on the Apache Spark engine.

Multiple different suites are included for measuring the performance of each algorithm separately on a variety of inputs or combined together.

Algorithms
========

* Block-Nested Loop
* Sort Filter Skyline
* Bitmap (WIP)

Getting started
========
Go to the [examples directory]() to see how easy it easy to use the API.

Third-party libraries
========
 * [Apache Spark 1.1.0](https://spark.apache.org/)

License
========

This application itself is released under **MIT** license, see [LICENSE](./LICENSE).

All 3rd party open sourced libs distributed with this application are still under their own license.
