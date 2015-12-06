package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.performance.result.PointDataFile;

import java.util.List;

public class Settings {

    @Parameter(names = {"-a", "-algorithm"}, description = "The algorithm(s) to use", required = true,
            converter = SkylineAlgorithmConverter.class)
    private List<SkylineAlgorithm> algorithms;
    @Parameter(names = {"-f", "-file"}, description = "The filepath(s) of the points", required = true,
            validateWith = PointDataFileValidator.class, converter = PointDataFileConverter.class)
    private List<PointDataFile> filepaths;
    @Parameter(names = {"-t", "-times"}, description = "How many times to run per algorithm and file combination",
            required = true, validateWith = BiggerThanZeroIntegerValidator.class)
    private Integer times;
    @Parameter(names = {"-o", "-output"}, description = "The output file with the results, can only be .txt or .xls",
            validateWith = OutputFileValidator.class)
    private String outputPath;
    @Parameter(names = "--help", help = true)
    private boolean help;

    public static Settings fromArgs(String[] args) {
        Settings settings = new Settings();
        JCommander jCommander = new JCommander(settings, args);
        jCommander.setProgramName("SkySpark performance testing");
        if (settings.isHelp()) {
            jCommander.usage();
            return null;
        }

        return settings;
    }

    public List<SkylineAlgorithm> getAlgorithms() {
        return algorithms;
    }

    public List<PointDataFile> getPointDataFiles() {
        return filepaths;
    }

    public Integer getTimes() {
        return times;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public boolean isHelp() {
        return help;
    }
}
