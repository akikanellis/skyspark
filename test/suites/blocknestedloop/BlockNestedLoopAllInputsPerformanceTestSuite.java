package suites.blocknestedloop;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 * @author Dimitris Kanellis
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({BlockNestedLoopSmallInputPerformanceTestSuite.class,
    BlockNestedLoopMediumInputPerformanceTestSuite.class,
    BlockNestedLoopBigInputPerformanceTestSuite.class})
public class BlockNestedLoopAllInputsPerformanceTestSuite {
}
