package com.github.dkanellis.skyspark.api.testUtils.dataMocks;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

public class PointsMock {

    private static List<Point2D> UNIFORM_2_10 = new ArrayList<Point2D>() {{
        add(new Point2D.Double(7384.475430902016, 6753.440980966086));
        add(new Point2D.Double(5132.274536706283, 176.07686372999513));
        add(new Point2D.Double(4691.353681108572, 8232.92056883113));
        add(new Point2D.Double(6292.759807632455, 3790.860474769524));
        add(new Point2D.Double(7250.357537607312, 2525.5019336970545));
        add(new Point2D.Double(755.6005425518632, 9555.368016130034));
        add(new Point2D.Double(4901.088081890441, 1031.5393572675114));
        add(new Point2D.Double(1963.7806084781564, 5979.674156591443));
        add(new Point2D.Double(6676.451493576449, 3373.7938261245263));
        add(new Point2D.Double(2604.9875068505676, 8312.417734885097));
    }};

    public static List<Point2D> getUniform210() {
        return new ArrayList<>(UNIFORM_2_10);
    }
}
