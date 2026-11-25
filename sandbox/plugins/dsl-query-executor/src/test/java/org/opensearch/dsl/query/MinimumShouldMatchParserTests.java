/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.test.OpenSearchTestCase;

public class MinimumShouldMatchParserTests extends OpenSearchTestCase {

    // --- Default behavior (null spec) ---

    public void testNullWithoutRequired() throws ConversionException {
        assertEquals(1, MinimumShouldMatchParser.calculateRequiredMatches(null, 3, false));
    }

    public void testNullWithRequired() throws ConversionException {
        assertEquals(0, MinimumShouldMatchParser.calculateRequiredMatches(null, 3, true));
    }

    // --- Integer specs ---

    public void testPositiveInteger() throws ConversionException {
        assertEquals(2, MinimumShouldMatchParser.calculateRequiredMatches("2", 5, false));
    }

    public void testNegativeInteger() throws ConversionException {
        assertEquals(4, MinimumShouldMatchParser.calculateRequiredMatches("-1", 5, false));
    }

    public void testIntegerExceedingTotal_noUpperClamp() throws ConversionException {
        // Legacy does NOT upper-clamp: "6" on total=4 returns 6.
        assertEquals(6, MinimumShouldMatchParser.calculateRequiredMatches("6", 4, false));
    }

    public void testLargeNegativeFloorClamp() throws ConversionException {
        // "-10" on total=4 -> 4 + (-10) = -6 -> floor clamp to 0.
        assertEquals(0, MinimumShouldMatchParser.calculateRequiredMatches("-10", 4, false));
    }

    // --- Percentage specs ---

    public void testPositivePercentage() throws ConversionException {
        assertEquals(2, MinimumShouldMatchParser.calculateRequiredMatches("70%", 4, false));
    }

    public void testNegativePercentage() throws ConversionException {
        assertEquals(3, MinimumShouldMatchParser.calculateRequiredMatches("-30%", 4, false));
    }

    public void testPaddedPercentage() throws ConversionException {
        // " 75% " on total=10 returns 7 (legacy trims whitespace).
        assertEquals(7, MinimumShouldMatchParser.calculateRequiredMatches(" 75% ", 10, false));
    }

    // --- Single combination specs ---

    public void testCombinationBelowThreshold() throws ConversionException {
        assertEquals(2, MinimumShouldMatchParser.calculateRequiredMatches("2<75%", 2, false));
    }

    public void testCombinationAboveThreshold() throws ConversionException {
        assertEquals(3, MinimumShouldMatchParser.calculateRequiredMatches("2<75%", 4, false));
    }

    public void testCombinationSingleEntry() throws ConversionException {
        // "5<50%" on total=4 -> total <= 5 -> return total (4).
        assertEquals(4, MinimumShouldMatchParser.calculateRequiredMatches("5<50%", 4, false));
    }

    // --- Multiple combination specs ---

    public void testMultipleCombinationsLow() throws ConversionException {
        assertEquals(3, MinimumShouldMatchParser.calculateRequiredMatches("3<-1 5<50%", 3, false));
    }

    public void testMultipleCombinationsMid() throws ConversionException {
        assertEquals(3, MinimumShouldMatchParser.calculateRequiredMatches("3<-1 5<50%", 4, false));
    }

    public void testMultipleCombinationsHigh() throws ConversionException {
        assertEquals(3, MinimumShouldMatchParser.calculateRequiredMatches("3<-1 5<50%", 6, false));
    }

    public void testMultipleCombinationsFromTestTable() throws ConversionException {
        // "3<-1 5<50%" on total=8 returns 4.
        assertEquals(4, MinimumShouldMatchParser.calculateRequiredMatches("3<-1 5<50%", 8, false));
    }

    // --- Space-around-operator normalization (legacy parity) ---

    public void testSpaceAroundOperator() throws ConversionException {
        // "3 < 75%" on total=10 returns 7 (spaces around < are normalized by legacy).
        assertEquals(7, MinimumShouldMatchParser.calculateRequiredMatches("3 < 75%", 10, false));
    }

    // --- Malformed specs: ConversionException ---

    public void testTrailingLessThanThrows() {
        // "5<" throws ArrayIndexOutOfBoundsException in legacy when total > threshold (parts[1] after split).
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("5<", 6, false)
        );
        assertTrue(ex.getMessage().contains("5<"));
    }

    public void testLeadingLessThanThrows() {
        // "<5" throws NumberFormatException in legacy (Integer.parseInt("") on empty parts[0]).
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("<5", 4, false)
        );
        assertTrue(ex.getMessage().contains("<5"));
    }

    public void testDoubleLessThanThrows() {
        // "5<<3" throws NumberFormatException in legacy (recursive call with "" from empty split part).
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("5<<3", 6, false)
        );
        assertTrue(ex.getMessage().contains("5<<3"));
    }

    public void testAlphabeticSpecThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("abc", 4, false)
        );
        assertTrue(ex.getMessage().contains("abc"));
    }

    public void testNaNPercentageThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("NaN%", 4, false)
        );
        assertTrue(ex.getMessage().contains("NaN%"));
    }

    public void testInfinityPercentageThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("Infinity%", 4, false)
        );
        assertTrue(ex.getMessage().contains("Infinity%"));
    }

    public void testFractionalPercentageThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("70.5%", 4, false)
        );
        assertTrue(ex.getMessage().contains("70.5%"));
    }

    public void testEmptyStringThrows() {
        // Legacy parity: "" is not equivalent to null; legacy throws NumberFormatException on "".
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("", 4, false)
        );
        assertTrue(ex.getMessage().contains("\"\""));
    }
}
