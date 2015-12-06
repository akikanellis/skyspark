package com.github.dkanellis.skyspark.performance;

/**
 * @author Dimitris Kanellis
 */
public abstract class AbstractPerformanceTest {
//    private static final int TIMES_TO_RUN = 1;
//
//    private static SparkContextWrapper sparkContext;
//    private static List<File> inputFiles;
//    private static List<File> expectedResults;
//
//    private static List<Result> performanceResults;
//
//    public static void init(String datasetFolder, String expectedResultsFolder,
//                            String appName) {
//        inputFiles = getListOfFilesFromFolder(datasetFolder);
//        expectedResults = getListOfFilesFromFolder(expectedResultsFolder);
//        sparkContext = new SparkContextWrapper(appName, "local[4]");
//        performanceResults = new ArrayList<>();
//    }
//
//    private static List<File> getListOfFilesFromFolder(String folderName) {
//        File folder = new File(folderName);
//        return Arrays.asList(folder.listFiles());
//    }
//
//    public static void addPerformanceResult(Result result) {
//        performanceResults.add(result);
//    }
//
//    public static void addAllPerformanceResults(List<Result> results) {
//        performanceResults.addAll(results);
//    }
//
//    public static void stopSparkContext() {
//        sparkContext.stop();
//    }
//
//    public static void printResults() {
//        for (Result performanceResult : performanceResults) {
//            System.out.println(performanceResult);
//        }
//    }
//
//    public static List<File> getInputFiles() {
//        return inputFiles;
//    }
//
//    public static List<File> getExpectedResultsFiles() {
//        return expectedResults;
//    }
//
//    public static SparkContextWrapper getSparkContext() {
//        return sparkContext;
//    }
//
//    public static int getTimesToRun() {
//        return TIMES_TO_RUN;
//    }
//
//    public static List<Result> getPerformanceResults() {
//        return performanceResults;
//    }
}
