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

        expectThrows(NumberFormatException.class, () -> SemverRange.fromString("$1.2.3"));
    }
}
