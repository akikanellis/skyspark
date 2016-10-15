package com.akikanellis.skyspark.performance.parsing;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.performance.Dates;
import com.akikanellis.skyspark.performance.result.PointDataFile;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.List;

public class Settings {

    @Parameter(names = {"-a", "-algorithm"}, description = "The algorithm(s) to use", required = true,
            converter = SkylineAlgorithmConverter.class)
    private List<SkylineAlgorithm> algorithms;

    @Parameter(names = {"-f", "-file"}, description = "The filepath(s) of the points", required = true,
            validateWith = PointDataFileValidator.class, converter = PointDataFileConverter.class)
    private List<PointDataFile> filepaths;

    @Parameter(names = {"-t", "-times"}, description = "How many times to run per algorithm and file combination",
            validateWith = BiggerThanZeroIntegerValidator.class)
    private Integer times = 1;

    @Parameter(names = {"-o", "-output"}, description = "The output file with the results, can only be .txt or .xls",
            validateWith = OutputFileValidator.class)
    private String outputPath = "Results of " + Dates.nowFormatted() + ".xls";

    @Parameter(names = {"-s", "-slaves"}, description = "The number of slaves used")
    private int numberOfSlaves = 0;

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

    public int getNumberOfSlaves() {
        return numberOfSlaves;
    }

    public boolean isHelp() {
        return help;
    }
}
