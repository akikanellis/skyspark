package com.akikanellis.skyspark.api.algorithms.bnl;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FlagTest {
    private Flag flag;

    @Before public void beforeEach() { flag = new Flag(false, true, false, true) /* 0101 */; }

    @Test public void equalsContract_isCorrect() { EqualsVerifier.forClass(Flag.class).verify(); }

    @Test public void gettingBit_withLessThanZeroIndex_throwsIndexOutOfBounds() {
        assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(() -> flag.bit(-1));
    }

    @Test public void gettingBit_withBiggerThanSizeIndex_throwsIndexOutOfBounds() {
        assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(() -> flag.bit(4));
    }

    @Test public void gettingBit_withZeroIndex_retrievesFirstBit() {
        assertThat(flag.bit(0)).isEqualTo(false);
    }

    @Test public void gettingBit_withThreeIndex_retrievesLastBit() {
        assertThat(flag.bit(3)).isEqualTo(true);
    }
}