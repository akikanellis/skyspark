package suites.blocknestedloop;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import performance.blocknestedloop.AbstractPerformanceTest;
import performance.blocknestedloop.BlockNestedLoopPerformanceTest;

/**
 *
 * @author Dimitris Kanellis
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({BlockNestedLoopPerformanceTest.class})
public class BlockNestedLoopMediumInputPerformanceTestSuite {

    private final static String DATASETS_FOLDER = "data/datasets/medium";
    private final static String EXPECTED_RESULTS_FOLDER = "data/expected results/medium";

    @BeforeClass
    public static void setUpClass() {
        AbstractPerformanceTest.init(
                DATASETS_FOLDER,
                EXPECTED_RESULTS_FOLDER,
                "BlockNestedLoopMediumInputPerformanceTestSuite");
    }

    @AfterClass
    public static void tearDownClass() throws InterruptedException {
        AbstractPerformanceTest.stopSparkContext();
        AbstractPerformanceTest.printResults();
    }
}
