package com.github.dkanellis.skyspark.performance;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.performance.parsing.SkylineAlgorithmConverter;

import java.util.List;

public class Settings {

    @Parameter(names = {"-a", "-algorithm"}, description = "The algorithm(s) to use", required = true,
            converter = SkylineAlgorithmConverter.class)
    private List<SkylineAlgorithm> algorithms;
    @Parameter(names = {"-f", "-file"}, description = "The file(s) of the points", required = true)
    private List<String> files;
    @Parameter(names = {"-t", "-times"}, description = "How many times to run per algorithm and file combination", required = true)
    private Integer times;
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

    public List<String> getFiles() {
        return files;
    }

    public Integer getTimes() {
        return times;
    }

    public boolean isHelp() {
        return help;
    }


}
