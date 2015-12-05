package performance;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.performance.PerformanceResult;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public abstract class AbstractPerformanceTest {
    private static final int TIMES_TO_RUN = 1;

    private static SparkContextWrapper sparkContext;
    private static List<File> inputFiles;
    private static List<File> expectedResults;

    private static List<PerformanceResult> performanceResults;

    public static void init(String datasetFolder, String expectedResultsFolder,
                            String appName) {
        inputFiles = getListOfFilesFromFolder(datasetFolder);
        expectedResults = getListOfFilesFromFolder(expectedResultsFolder);
        sparkContext = new SparkContextWrapper(appName, "local[4]");
        performanceResults = new ArrayList<>();
    }

    private static List<File> getListOfFilesFromFolder(String folderName) {
        File folder = new File(folderName);
        return Arrays.asList(folder.listFiles());
    }

    public static void addPerformanceResult(PerformanceResult result) {
        performanceResults.add(result);
    }

    public static void addAllPerformanceResults(List<PerformanceResult> results) {
        performanceResults.addAll(results);
    }

    public static void stopSparkContext() {
        sparkContext.stop();
    }

    public static void printResults() {
        for (PerformanceResult performanceResult : performanceResults) {
            System.out.println(performanceResult);
        }
    }

    public static List<File> getInputFiles() {
        return inputFiles;
    }

    public static List<File> getExpectedResultsFiles() {
        return expectedResults;
    }

    public static SparkContextWrapper getSparkContext() {
        return sparkContext;
    }

    public static int getTimesToRun() {
        return TIMES_TO_RUN;
    }

    public static List<PerformanceResult> getPerformanceResults() {
        return performanceResults;
    }
}
