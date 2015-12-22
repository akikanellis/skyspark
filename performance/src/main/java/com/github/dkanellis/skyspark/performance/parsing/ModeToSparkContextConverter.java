package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;

public class ModeToSparkContextConverter implements IStringConverter<JavaSparkContext> {
    @Override
    public JavaSparkContext convert(String value) {
        value = checkNotEmpty(value).toLowerCase();

        SparkConf sparkConf = new SparkConf().setAppName("Performance evaluation for Skyline computing");
        switch (value) {
            case "local":
                sparkConf.setMaster("local[*]");
                break;
            case "cluster":
                break;
            default:
                throw new ParameterException("Wrong mode value: " + value);
        }

        return new JavaSparkContext(sparkConf);
    }
}
