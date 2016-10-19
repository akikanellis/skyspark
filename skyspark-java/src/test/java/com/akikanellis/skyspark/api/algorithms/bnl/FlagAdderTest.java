package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.test_utils.SparkContextRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FlagAdderTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    @Mock private MedianFinder medianFinder;
    private FlagAdder flagAdder;

    @Before public void beforeEach() { flagAdder = new FlagAdder(medianFinder); }
}