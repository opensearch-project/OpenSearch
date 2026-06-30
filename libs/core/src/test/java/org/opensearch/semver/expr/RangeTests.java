/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.semver.expr;

import org.opensearch.Version;
import org.opensearch.test.OpenSearchTestCase;

public class RangeTests extends OpenSearchTestCase {

    public void testDefaultConstructor() {
        Range range = new Range();
        assertEquals(Version.fromString("0.0.0"), range.getLowerBound());
        assertEquals(Version.fromString("99.99.99"), range.getUpperBound());
        assertTrue(range.isIncludeLower());
        assertTrue(range.isIncludeUpper());

    }

    public void testRangeEvaluation() {
        Version lowerBound = Version.fromString("2.3.0");
        Version upperBound = Version.fromString("2.7.0");

        // Test inclusive range
        Range inclusiveRange = new Range(lowerBound, upperBound, true, true);
        assertTrue(inclusiveRange.evaluate(null, Version.fromString("2.3.0")));
        assertTrue(inclusiveRange.evaluate(null, Version.fromString("2.5.0")));
        assertTrue(inclusiveRange.evaluate(null, Version.fromString("2.7.0")));
        assertFalse(inclusiveRange.evaluate(null, Version.fromString("2.2.9")));
        assertFalse(inclusiveRange.evaluate(null, Version.fromString("2.7.1")));

        // Test exclusive range
        Range exclusiveRange = new Range(lowerBound, upperBound, false, false);
        assertFalse(exclusiveRange.evaluate(null, Version.fromString("2.3.0")));
        assertTrue(exclusiveRange.evaluate(null, Version.fromString("2.5.0")));
        assertFalse(exclusiveRange.evaluate(null, Version.fromString("2.7.0")));
        assertFalse(exclusiveRange.evaluate(null, Version.fromString("2.2.9")));
        assertFalse(exclusiveRange.evaluate(null, Version.fromString("2.7.1")));
    }

    public void testInvalidRanges() {
        Version lowerBound = Version.fromString("2.3.0");
        Version upperBound = Version.fromString("2.7.0");

        // Test null bounds
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new Range(null, upperBound, true, true));
        assertEquals("Lower bound cannot be null", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new Range(lowerBound, null, true, true));
        assertEquals("Upper bound cannot be null", ex.getMessage());

        // Test invalid range (upper < lower)
        ex = expectThrows(IllegalArgumentException.class, () -> new Range(upperBound, lowerBound, true, true));
        assertEquals("Lower bound must be less than or equal to upper bound", ex.getMessage());
    }

    public void testMixedBoundTypes() {
        Version lowerBound = Version.fromString("2.3.0");
        Version upperBound = Version.fromString("2.7.0");

        // Test inclusive lower, exclusive upper
        Range mixedRange1 = new Range(lowerBound, upperBound, true, false);
        assertTrue(mixedRange1.evaluate(null, Version.fromString("2.3.0")));
        assertTrue(mixedRange1.evaluate(null, Version.fromString("2.6.9")));
        assertFalse(mixedRange1.evaluate(null, Version.fromString("2.7.0")));

        // Test exclusive lower, inclusive upper
        Range mixedRange2 = new Range(lowerBound, upperBound, false, true);
        assertFalse(mixedRange2.evaluate(null, Version.fromString("2.3.0")));
        assertTrue(mixedRange2.evaluate(null, Version.fromString("2.3.1")));
        assertTrue(mixedRange2.evaluate(null, Version.fromString("2.7.0")));
    }

    public void testRangeAccessors() {
        Version lowerBound = Version.fromString("2.3.0");
        Version upperBound = Version.fromString("2.7.0");
        Range range = new Range(lowerBound, upperBound, true, false);

        assertEquals("Lower bound should match", lowerBound, range.getLowerBound());
        assertEquals("Upper bound should match", upperBound, range.getUpperBound());
        assertTrue("Should be inclusive lower", range.isIncludeLower());
        assertFalse("Should be exclusive upper", range.isIncludeUpper());
    }

    public void testUpdateRangeNullCheck() {
        Range range = new Range(
            Version.fromString("1.0.0"),
            Version.fromString("2.0.0"),
            true,  // includeLower
            true   // includeUpper
        );

        // Test that updateRange throws IllegalArgumentException for null input
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> range.updateRange(null));
        assertEquals("Range cannot be null", ex.getMessage());
    }

}
