package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.base.Objects;
import scala.Tuple2;

class Rankings {

    private final Long firstDimensionRanking;
    private final Long secondDimensionRanking;

    Rankings(Long firstDimensionRanking, Long secondDimensionRanking) {
        this.firstDimensionRanking = firstDimensionRanking;
        this.secondDimensionRanking = secondDimensionRanking;
    }

    static Rankings fromTuple(Tuple2<Long, Long> rankingTuple) {
        return new Rankings(rankingTuple._1(), rankingTuple._2());
    }

    public static Rankings reduceByOne(Rankings rankings) {
        return new Rankings(rankings.firstDimensionRanking - 1, rankings.secondDimensionRanking - 1);
    }

    Long getFirstDimensionRanking() {
        return firstDimensionRanking;
    }

    Long getSecondDimensionRanking() {
        return secondDimensionRanking;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rankings rankings = (Rankings) o;
        return Objects.equal(firstDimensionRanking, rankings.firstDimensionRanking) &&
                Objects.equal(secondDimensionRanking, rankings.secondDimensionRanking);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(firstDimensionRanking, secondDimensionRanking);
    }
}
