/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.semver;

import org.opensearch.test.OpenSearchTestCase;

public class SemverRangeTests extends OpenSearchTestCase {

    public void testRangeWithEqualsOperator() {
        SemverRange range = SemverRange.fromString("=1.2.3");
        assertEquals(range.getRangeOperator(), SemverRange.RangeOperator.EQ);
        assertTrue(range.isSatisfiedBy("1.2.3"));
        assertFalse(range.isSatisfiedBy("1.2.4"));
        assertFalse(range.isSatisfiedBy("1.3.3"));
        assertFalse(range.isSatisfiedBy("2.2.3"));
    }

    public void testRangeWithDefaultOperator() {
        SemverRange range = SemverRange.fromString("1.2.3");
        assertEquals(range.getRangeOperator(), SemverRange.RangeOperator.DEFAULT);
        assertTrue(range.isSatisfiedBy("1.2.3"));
        assertFalse(range.isSatisfiedBy("1.2.4"));
        assertFalse(range.isSatisfiedBy("1.3.3"));
        assertFalse(range.isSatisfiedBy("2.2.3"));
    }

    public void testRangeWithTildeOperator() {
        SemverRange range = SemverRange.fromString("~2.3.4");
        assertEquals(range.getRangeOperator(), SemverRange.RangeOperator.TILDE);
        assertTrue(range.isSatisfiedBy("2.3.4"));
        assertTrue(range.isSatisfiedBy("2.3.5"));
        assertTrue(range.isSatisfiedBy("2.3.12"));

        assertFalse(range.isSatisfiedBy("2.3.0"));
        assertFalse(range.isSatisfiedBy("2.3.3"));
        assertFalse(range.isSatisfiedBy("2.4.0"));
        assertFalse(range.isSatisfiedBy("3.0.0"));
    }

    public void testRangeWithCaretOperator() {
        SemverRange range = SemverRange.fromString("^2.3.4");
        assertEquals(range.getRangeOperator(), SemverRange.RangeOperator.CARET);
        assertTrue(range.isSatisfiedBy("2.3.4"));
        assertTrue(range.isSatisfiedBy("2.3.5"));
        assertTrue(range.isSatisfiedBy("2.4.12"));

        assertFalse(range.isSatisfiedBy("2.3.3"));
        assertFalse(range.isSatisfiedBy("3.0.0"));
    }

    public void testInvalidRanges() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString(""));
        assertEquals("Version cannot be empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("1"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("1.2"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("="));
        assertEquals("Version cannot be empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("=1"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("=1.2"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("~"));
        assertEquals("Version cannot be empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("~1"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("~1.2"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("^"));
        assertEquals("Version cannot be empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("^1"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("^1.2"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("$"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("$1"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        ex = expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("$1.2"));
        assertTrue(ex.getMessage().contains("the version needs to contain major, minor, and revision, and optionally the build"));

        expectThrows(IllegalArgumentException.class, () -> SemverRange.fromString("$1.2.3"));

        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("[2.3.0]"));
        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("[2.3.0,]"));
        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("[,2.7.0]"));
        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("2.3.0,2.7.0"));
        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("[2.7.0,2.3.0]"));
    }

    public void testInclusiveRange() {
        SemverRange range = SemverRange.fromString("[2.3.0,2.7.0]");

        // Test lower bound
        assertTrue("Should include lower bound", range.isSatisfiedBy("2.3.0"));

        // Test upper bound
        assertTrue("Should include upper bound", range.isSatisfiedBy("2.7.0"));

        // Test middle values
        assertTrue("Should include values in range", range.isSatisfiedBy("2.5.0"));
        assertTrue("Should include patch versions", range.isSatisfiedBy("2.4.1"));

        // Test out of range values
        assertFalse("Should exclude values below range", range.isSatisfiedBy("2.2.9"));
        assertFalse("Should exclude values above range", range.isSatisfiedBy("2.7.1"));
    }

    public void testExclusiveRange() {
        SemverRange range = SemverRange.fromString("(2.3.0,2.7.0)");

        // Test bounds
        assertFalse("Should exclude lower bound", range.isSatisfiedBy("2.3.0"));
        assertFalse("Should exclude upper bound", range.isSatisfiedBy("2.7.0"));

        // Test middle values
        assertTrue("Should include values in range", range.isSatisfiedBy("2.5.0"));
        assertTrue("Should include values near lower bound", range.isSatisfiedBy("2.3.1"));
        assertTrue("Should include values near upper bound", range.isSatisfiedBy("2.6.9"));

        // Test out of range values
        assertFalse("Should exclude values below range", range.isSatisfiedBy("2.2.9"));
        assertFalse("Should exclude values above range", range.isSatisfiedBy("2.7.1"));
    }

    public void testMixedRanges() {
        // Test inclusive lower bound, exclusive upper bound
        SemverRange range1 = SemverRange.fromString("[2.3.0,2.7.0)");
        assertTrue("Should include lower bound", range1.isSatisfiedBy("2.3.0"));
        assertFalse("Should exclude upper bound", range1.isSatisfiedBy("2.7.0"));
        assertTrue("Should include values in range", range1.isSatisfiedBy("2.6.9"));

        // Test exclusive lower bound, inclusive upper bound
        SemverRange range2 = SemverRange.fromString("(2.3.0,2.7.0]");
        assertFalse("Should exclude lower bound", range2.isSatisfiedBy("2.3.0"));
        assertTrue("Should include upper bound", range2.isSatisfiedBy("2.7.0"));
        assertTrue("Should include values in range", range2.isSatisfiedBy("2.3.1"));
    }

    public void testInvalidRangeConstructions() {
        // Test invalid range constructions
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> SemverRange.fromString("[2.0.0,1.0.0]")  // Lower bound greater than upper bound
        );
        assertTrue(ex.getMessage().contains("Lower bound must be less than or equal to upper bound"));

        // Test malformed ranges
        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("[2.0.0]"));
        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("[2.0.0,]"));
        assertThrows(IllegalArgumentException.class, () -> SemverRange.fromString("[,2.0.0]"));
    }

    public void testEqualsAndHashCode() {
        // Test inclusive ranges [a,b]
        SemverRange inclusiveRange1 = SemverRange.fromString("[1.0.0,2.0.0]");
        SemverRange inclusiveRange2 = SemverRange.fromString("[1.0.0,2.0.0]");

        assertTrue("Same inclusive range should be equal to itself", inclusiveRange1.equals(inclusiveRange1));
        assertTrue("Identical inclusive ranges should be equal", inclusiveRange1.equals(inclusiveRange2));
        assertEquals("Equal inclusive ranges should have same hashcode", inclusiveRange1.hashCode(), inclusiveRange2.hashCode());

        // Test exclusive ranges (a,b)
        SemverRange exclusiveRange1 = SemverRange.fromString("(1.0.0,2.0.0)");
        SemverRange exclusiveRange2 = SemverRange.fromString("(1.0.0,2.0.0)");

        assertTrue("Same exclusive range should be equal to itself", exclusiveRange1.equals(exclusiveRange1));
        assertTrue("Identical exclusive ranges should be equal", exclusiveRange1.equals(exclusiveRange2));
        assertEquals("Equal exclusive ranges should have same hashcode", exclusiveRange1.hashCode(), exclusiveRange2.hashCode());

        // Test mixed ranges [a,b) and (a,b]
        SemverRange leftInclusiveRange1 = SemverRange.fromString("[1.0.0,2.0.0)");
        SemverRange leftInclusiveRange2 = SemverRange.fromString("[1.0.0,2.0.0)");
        SemverRange rightInclusiveRange1 = SemverRange.fromString("(1.0.0,2.0.0]");
        SemverRange rightInclusiveRange2 = SemverRange.fromString("(1.0.0,2.0.0]");

        assertTrue("Same left-inclusive ranges should be equal", leftInclusiveRange1.equals(leftInclusiveRange2));
        assertTrue("Same right-inclusive ranges should be equal", rightInclusiveRange1.equals(rightInclusiveRange2));
        assertEquals(
            "Equal left-inclusive ranges should have same hashcode",
            leftInclusiveRange1.hashCode(),
            leftInclusiveRange2.hashCode()
        );
        assertEquals(
            "Equal right-inclusive ranges should have same hashcode",
            rightInclusiveRange1.hashCode(),
            rightInclusiveRange2.hashCode()
        );

        // Test inequality between different range types
        assertFalse("Inclusive range should not equal exclusive range", inclusiveRange1.equals(exclusiveRange1));
        assertFalse("Inclusive range should not equal left-inclusive range", inclusiveRange1.equals(leftInclusiveRange1));
        assertFalse("Inclusive range should not equal right-inclusive range", inclusiveRange1.equals(rightInclusiveRange1));
        assertFalse("Exclusive range should not equal left-inclusive range", exclusiveRange1.equals(leftInclusiveRange1));
        assertFalse("Left-inclusive range should not equal right-inclusive range", leftInclusiveRange1.equals(rightInclusiveRange1));

        // Test different version ranges
        SemverRange differentRange = SemverRange.fromString("[1.0.0,3.0.0]");
        assertFalse("Ranges with different versions should not be equal", inclusiveRange1.equals(differentRange));

        // Test null and different types
        assertFalse("Range should not equal null", inclusiveRange1.equals(null));
        assertFalse("Range should not equal different type", inclusiveRange1.equals("not a range"));
    }

    public void testRangeOperatorExpression() {
        // Test that expressions are properly assigned for different operators
        SemverRange eqRange = SemverRange.fromString("=2.0.0");
        SemverRange tildeRange = SemverRange.fromString("~2.0.0");
        SemverRange caretRange = SemverRange.fromString("^2.0.0");

        assertEquals(SemverRange.RangeOperator.EQ, eqRange.getRangeOperator());
        assertEquals(SemverRange.RangeOperator.TILDE, tildeRange.getRangeOperator());
        assertEquals(SemverRange.RangeOperator.CARET, caretRange.getRangeOperator());
    }

    public void testRangeToStringConsistency() {
        // Test that toString produces consistent output
        String[] testRanges = { "[2.0.0,3.0.0]", "(2.0.0,3.0.0)", "[2.0.0,3.0.0)", "(2.0.0,3.0.0]", "^2.0.0", "~2.0.0", "=2.0.0" };

        for (String rangeStr : testRanges) {
            SemverRange range = SemverRange.fromString(rangeStr);
            assertEquals("toString should match original string", rangeStr, range.toString());
        }
    }

    public void testRangeBoundaryConditions() {
        // Test edge cases for range boundaries
        SemverRange range = SemverRange.fromString("[2.0.0,3.0.0]");

        // Test exact boundaries
        assertTrue("Should include lower bound", range.isSatisfiedBy("2.0.0"));
        assertTrue("Should include upper bound", range.isSatisfiedBy("3.0.0"));

        // Test just outside boundaries
        assertFalse("Should exclude version before lower bound", range.isSatisfiedBy("1.9.99"));
        assertFalse("Should exclude version after upper bound", range.isSatisfiedBy("3.0.1"));
    }

    public void testRangeEvaluationConsistency() {
        // Test that range evaluation is consistent
        SemverRange range = SemverRange.fromString("[2.0.0,3.0.0]");

        // Test same version evaluation
        boolean result1 = range.isSatisfiedBy("2.5.0");
        boolean result2 = range.isSatisfiedBy("2.5.0");
        assertEquals("Same version should evaluate consistently", result1, result2);

    }

    public void testRangeToString() {
        // Test that toString produces the same string that was parsed
        String[] ranges = { "[2.3.0,2.7.0]", "(2.3.0,2.7.0)", "[2.3.0,2.7.0)", "(2.3.0,2.7.0]" };

        for (String rangeStr : ranges) {
            SemverRange range = SemverRange.fromString(rangeStr);
            assertEquals("toString should match original string", rangeStr, range.toString());
        }
    }

    public void testRangeEquality() {
        SemverRange range1 = SemverRange.fromString("[2.3.0,2.7.0]");
        SemverRange range2 = SemverRange.fromString("[2.3.0,2.7.0]");
        SemverRange range3 = SemverRange.fromString("(2.3.0,2.7.0]");

        assertEquals("Identical ranges should be equal", range1, range2);
        assertNotEquals("Different ranges should not be equal", range1, range3);
        assertNotEquals("Range should not equal null", null, range1);
    }

    public void testVersionEdgeCases() {
        SemverRange range = SemverRange.fromString("[2.0.0,3.0.0]");

        // Test major version boundaries
        assertTrue(range.isSatisfiedBy("2.0.0"));
        assertTrue(range.isSatisfiedBy("2.99.99"));
        assertTrue(range.isSatisfiedBy("3.0.0"));
        assertFalse(range.isSatisfiedBy("1.99.99"));
        assertFalse(range.isSatisfiedBy("3.0.1"));

    }

}
