package performance;

import com.github.dkanellis.skyspark.api.algorithms.factories.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.performance.PerformanceResult;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Dimitris Kanellis
 */
public class BlockNestedLoopPerformanceTest {

    private static SparkContextWrapper sparkContext;
    private static SkylineAlgorithmFactory algorithmFactory;
    private static List<File> inputFiles;
    private static List<File> expectedResultsFiles;
    private static int timesToRun;

    private static List<PerformanceResult> results;

    public BlockNestedLoopPerformanceTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        sparkContext = AbstractPerformanceTest.getSparkContext();
        algorithmFactory = AbstractPerformanceTest.getAlgorithmFactory();
        inputFiles = AbstractPerformanceTest.getInputFiles();
        expectedResultsFiles = AbstractPerformanceTest.getExpectedResultsFiles();
        timesToRun = AbstractPerformanceTest.getTimesToRun();
        results = new ArrayList<>();
    }

    @AfterClass
    public static void tearDownClass() {
        AbstractPerformanceTest.addAllPerformanceResults(results);
    }

    @Test
    public void shouldReturnCorrectUniformSkylines() throws FileNotFoundException {
        File inputFile = getFileContainingKeyword(inputFiles, "uniform");
        File expResultFile = getFileContainingKeyword(expectedResultsFiles, "uniform");

        PerformanceResult finalResult = getTotalRuntime(inputFile, expResultFile);
        results.add(finalResult);
    }

    @Test
    public void shouldReturnCorrectCorrelatedSkylines() throws FileNotFoundException {
        File inputFile = getFileContainingKeyword(inputFiles, "correl");
        File expResultFile = getFileContainingKeyword(expectedResultsFiles, "correl");

        PerformanceResult finalResult = getTotalRuntime(inputFile, expResultFile);
        results.add(finalResult);
    }

    @Test
    public void shouldReturnCorrectAnticorrelatedSkylines() throws FileNotFoundException {
        File inputFile = getFileContainingKeyword(inputFiles, "anticor");
        File expResultFile = getFileContainingKeyword(expectedResultsFiles, "anticor");

        PerformanceResult finalResult = getTotalRuntime(inputFile, expResultFile);
        results.add(finalResult);
    }

    private File getFileContainingKeyword(List<File> filesToSearchIn, String keyword) throws FileNotFoundException {
        String lowerCaseKeyword = keyword.toLowerCase();
        for (File file : filesToSearchIn) {
            String fileName = file.getName();
            String lowerCaseFileName = fileName.toLowerCase();
            if (lowerCaseFileName.contains(lowerCaseKeyword)) {
                return file;
            }
        }
        throw new FileNotFoundException("Keyword: " + keyword + " was not found in " + getFolderPath());
    }

    private String getFolderPath() {
        return inputFiles.get(0).getParent();
    }

    private PerformanceResult getTotalRuntime(File inputFile, File expResultFile) {
        String inputFilePath = inputFile.getAbsolutePath();
        List<Point2D> expResult = getPointsFromFile(expResultFile);
        SkylineAlgorithm bnl = algorithmFactory.getBlockNestedLoop(sparkContext);
        
        PerformanceResult performanceResult = new PerformanceResult("Block Nested Loop", inputFile);
        for (int i = 0; i < timesToRun; i++) {
            long startTime = System.currentTimeMillis();
            List<Point2D> result = bnl.getSkylinePoints(inputFilePath);
            long endTime = System.currentTimeMillis();
            long totalDuration = endTime - startTime;
            performanceResult.addResult(totalDuration);

            expResult.removeAll(result);

            assertTrue(expResult.isEmpty());
        }
        return performanceResult;
    }

    private List<Point2D> getPointsFromFile(File file) {
        TextFileToPointRDD txtToPoints = new TextFileToPointRDD(sparkContext);
        JavaRDD<Point2D> pointsRDD = txtToPoints.getPointRDDFromTextFile(file.getAbsolutePath(), " ");

        return pointsRDD.collect();
    }
}
