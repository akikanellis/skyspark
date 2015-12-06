package com.github.dkanellis.skyspark.performance;

public class Main {

    public static void main(String[] args) {
        Settings settings = Settings.fromArgs(args);
        if (settings == null) {
            return;
        }

        System.out.println(settings);
    }
}
