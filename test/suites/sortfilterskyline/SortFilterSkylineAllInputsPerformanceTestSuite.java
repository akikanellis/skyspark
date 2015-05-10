package suites.sortfilterskyline;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 * @author Dimitris Kanellis
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({SortFilterSkylineSmallInputPerformanceTestSuite.class,
    SortFilterSkylineMediumInputPerformanceTestSuite.class,
    SortFilterSkylineBigInputPerformanceTestSuite.class})
public class SortFilterSkylineAllInputsPerformanceTestSuite {
}
