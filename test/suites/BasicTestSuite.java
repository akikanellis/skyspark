package suites;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.BlockNestedLoopTest;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SortFilterSkylineTest;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.TextFileToPointRDDTest;
import com.github.dkanellis.skyspark.api.math.point.FlagPointPairProducerTest;
import com.github.dkanellis.skyspark.api.math.point.PointFlagTest;
import com.github.dkanellis.skyspark.api.math.point.PointUtils;
import com.github.dkanellis.skyspark.api.math.point.comparators.DominationComparatorTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import testcategories.BasicTest;

/**
 *
 * @author Dimitris Kanellis
 */
@RunWith(Categories.class)
@IncludeCategory(BasicTest.class)
@Suite.SuiteClasses({
    BlockNestedLoopTest.class, SortFilterSkylineTest.class,
    TextFileToPointRDDTest.class, DominationComparatorTest.class,
    FlagPointPairProducerTest.class, PointFlagTest.class,
    PointUtils.class})
public class BasicTestSuite {
    
    public static SparkContextWrapper sparkContext;
    
    @Before
    public void setUp() throws Exception {
        System.out.println("Before suite");
        sparkContext = new SparkContextWrapper("BasicTestSuite", "local");
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        System.out.println("Before suite");
        sparkContext = new SparkContextWrapper("BasicTestSuite", "local");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        System.out.println("After suite");
        sparkContext.stop();
    }

}
