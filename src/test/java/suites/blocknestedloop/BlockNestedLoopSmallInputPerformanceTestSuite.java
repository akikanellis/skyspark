package suites.blocknestedloop;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import performance.AbstractPerformanceTest;
import performance.BlockNestedLoopPerformanceTest;

/**
 *
 * @author Dimitris Kanellis
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({BlockNestedLoopPerformanceTest.class})
public class BlockNestedLoopSmallInputPerformanceTestSuite {

    private final static String DATASETS_FOLDER = "data/datasets/small";
    private final static String EXPECTED_RESULTS_FOLDER = "data/expected results/small";

    @BeforeClass
    public static void setUpClass() {
        AbstractPerformanceTest.init(
                DATASETS_FOLDER,
                EXPECTED_RESULTS_FOLDER,
                "BlockNestedLoopSmallInputPerformanceTestSuite");
    }

    @AfterClass
    public static void tearDownClass() throws InterruptedException {
        AbstractPerformanceTest.stopSparkContext();
        AbstractPerformanceTest.printResults();
    }
}
