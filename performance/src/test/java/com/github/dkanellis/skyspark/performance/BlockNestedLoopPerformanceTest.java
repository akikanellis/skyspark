package com.github.dkanellis.skyspark.performance;

/**
 * @author Dimitris Kanellis
 */
public class BlockNestedLoopPerformanceTest {
//
//    private static SparkContextWrapper sparkContext;
//    private static List<File> inputFiles;
//    private static List<File> expectedResultsFiles;
//    private static int timesToRun;
//
//    private static List<Result> results;
//
//    public BlockNestedLoopPerformanceTest() {
//    }
//
//    @BeforeClass
//    public static void setUpClass() {
//        sparkContext = AbstractPerformanceTest.getSparkContext();
//        inputFiles = AbstractPerformanceTest.getInputFiles();
//        expectedResultsFiles = AbstractPerformanceTest.getExpectedResultsFiles();
//        timesToRun = AbstractPerformanceTest.getTimesToRun();
//        results = new ArrayList<>();
//    }
//
//    @AfterClass
//    public static void tearDownClass() {
//        AbstractPerformanceTest.addAllPerformanceResults(results);
//    }
//
//    @Test
//    public void shouldReturnCorrectUniformSkylines() throws FileNotFoundException {
//        File inputFile = getFileContainingKeyword(inputFiles, "uniform");
//        File expResultFile = getFileContainingKeyword(expectedResultsFiles, "uniform");
//
//        Result finalResult = getTotalRuntime(inputFile, expResultFile);
//        results.add(finalResult);
//    }
//
//    @Test
//    public void shouldReturnCorrectCorrelatedSkylines() throws FileNotFoundException {
//        File inputFile = getFileContainingKeyword(inputFiles, "correl");
//        File expResultFile = getFileContainingKeyword(expectedResultsFiles, "correl");
//
//        Result finalResult = getTotalRuntime(inputFile, expResultFile);
//        results.add(finalResult);
//    }
//
//    @Test
//    public void shouldReturnCorrectAnticorrelatedSkylines() throws FileNotFoundException {
//        File inputFile = getFileContainingKeyword(inputFiles, "anticor");
//        File expResultFile = getFileContainingKeyword(expectedResultsFiles, "anticor");
//
//        Result finalResult = getTotalRuntime(inputFile, expResultFile);
//        results.add(finalResult);
//    }
//
//    private File getFileContainingKeyword(List<File> filesToSearchIn, String keyword) throws FileNotFoundException {
//        String lowerCaseKeyword = keyword.toLowerCase();
//        for (File file : filesToSearchIn) {
//            String fileName = file.getName();
//            String lowerCaseFileName = fileName.toLowerCase();
//            if (lowerCaseFileName.contains(lowerCaseKeyword)) {
//                return file;
//            }
//        }
//        throw new FileNotFoundException("Keyword: " + keyword + " was not found in " + getFolderPath());
//    }
//
//    private String getFolderPath() {
//        return inputFiles.get(0).getParent();
//    }
//
//    private Result getTotalRuntime(File inputFile, File expResultFile) {
//        String inputFilePath = inputFile.getAbsolutePath();
//        List<Point2D> expResult = getPointsFromFile(expResultFile);
//        SkylineAlgorithm bnl = new BlockNestedLoop(sparkContext);
//
//        Result performanceResult = new Result("Block Nested Loop", inputFile);
//        for (int i = 0; i < timesToRun; i++) {
//            long startTime = System.currentTimeMillis();
//            List<Point2D> result = bnl.computeSkylinePoints(inputFilePath);
//            long endTime = System.currentTimeMillis();
//            long totalDuration = endTime - startTime;
//            performanceResult.addResult(totalDuration);
//
//            expResult.removeAll(result);
//        }
//        return performanceResult;
//    }
//
//    private List<Point2D> getPointsFromFile(File file) {
//        TextFileToPointRDD txtToPoints = new TextFileToPointRDD(sparkContext);
//        JavaRDD<Point2D> pointsRDD = txtToPoints.getPointRDDFromTextFile(file.getAbsolutePath(), " ");
//
//        return pointsRDD.collect();
//    }
}
