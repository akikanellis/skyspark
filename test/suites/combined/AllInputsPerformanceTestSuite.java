package suites.combined;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 * @author Dimitris Kanellis
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({SmallInputPerformanceTestSuite.class,
    MediumInputPerformanceTestSuite.class,
    BigInputPerformanceTestSuite.class})
public class AllInputsPerformanceTestSuite {
}
