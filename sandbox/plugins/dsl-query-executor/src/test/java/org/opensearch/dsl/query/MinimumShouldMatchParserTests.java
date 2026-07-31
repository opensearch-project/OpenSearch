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

    // Test integer parsing
    public void testParsePositiveInteger() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("2", 5, false);
        assertEquals(2, result);
    }

    public void testParseNegativeInteger() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("-1", 5, false);
        assertEquals(4, result); // 5 - 1 = 4
    }

    // Test percentage parsing
    public void testParsePositivePercentage() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("70%", 4, false);
        assertEquals(2, result); // floor(4 * 70 / 100) = 2
    }

    public void testParseNegativePercentage() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("-30%", 4, false);
        assertEquals(3, result); // 4 - floor(4 * 30 / 100) = 3
    }

    // Test combination parsing
    public void testParseCombinationBelowThreshold() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("2<75%", 2, false);
        assertEquals(2, result); // total <= 2, match all
    }

    public void testParseCombinationAboveThreshold() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("2<75%", 4, false);
        assertEquals(3, result); // total > 2, floor(4 * 75 / 100) = 3
    }

    // Test multiple combinations
    public void testParseMultipleCombinationsLow() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("3<-1 5<50%", 3, false);
        assertEquals(3, result); // total <= 3, match all
    }

    public void testParseMultipleCombinationsMid() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("3<-1 5<50%", 4, false);
        assertEquals(3, result); // 3 < 4 <= 5, so -1 = 4 - 1 = 3
    }

    public void testParseMultipleCombinationsHigh() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("3<-1 5<50%", 6, false);
        assertEquals(3, result); // total > 5, floor(6 * 50 / 100) = 3
    }

    // Test default behavior
    public void testDefaultWithoutMust() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches(null, 3, false);
        assertEquals(1, result); // No must clause, at least 1 should match
    }

    public void testDefaultWithMust() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches(null, 3, true);
        assertEquals(0, result); // Has must clause, should is optional
    }

    // Non-finite and fractional percentage rejection tests.
    // WHY: Legacy Queries.calculateMinShouldMatch (line ~199) uses Integer.parseInt for the
    // percentage numeric part, rejecting non-integer and non-finite values.

    public void testParsePercentageNaNThrowsConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("NaN%", 4, false)
        );
        assertTrue("Message must contain the offending value", ex.getMessage().contains("NaN%"));
    }

    public void testParsePercentageInfinityThrowsConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("Infinity%", 4, false)
        );
        assertTrue("Message must contain the offending value", ex.getMessage().contains("Infinity%"));
    }

    public void testParsePercentageNegativeInfinityThrowsConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("-Infinity%", 4, false)
        );
        assertTrue("Message must contain the offending value", ex.getMessage().contains("-Infinity%"));
    }

    public void testParsePercentageFractionalThrowsConversionException() {
        // Legacy Integer.parseInt rejects fractional values like "70.5".
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("70.5%", 4, false)
        );
        assertTrue("Message must contain the offending value", ex.getMessage().contains("70.5%"));
    }
}
