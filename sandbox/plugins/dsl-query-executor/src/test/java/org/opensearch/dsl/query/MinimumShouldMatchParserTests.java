/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.opensearch.test.OpenSearchTestCase;

public class MinimumShouldMatchParserTests extends OpenSearchTestCase {

    private BoolQueryTranslator translator = new BoolQueryTranslator(QueryRegistryFactory.create());

    // Test integer parsing
    public void testParsePositiveInteger() {
        int result = translator.calculateRequiredMatches("2", 5, false);
        assertEquals(2, result);
    }

    public void testParseNegativeInteger() {
        int result = translator.calculateRequiredMatches("-1", 5, false);
        assertEquals(4, result); // 5 - 1 = 4
    }

    // Test percentage parsing
    public void testParsePositivePercentage() {
        int result = translator.calculateRequiredMatches("70%", 4, false);
        assertEquals(2, result); // floor(4 * 0.7) = 2
    }

    public void testParseNegativePercentage() {
        int result = translator.calculateRequiredMatches("-30%", 4, false);
        assertEquals(3, result); // 4 - floor(4 * 0.3) = 3
    }

    // Test combination parsing
    public void testParseCombinationBelowThreshold() {
        int result = translator.calculateRequiredMatches("2<75%", 2, false);
        assertEquals(2, result); // total <= 2, match all
    }

    public void testParseCombinationAboveThreshold() {
        int result = translator.calculateRequiredMatches("2<75%", 4, false);
        assertEquals(3, result); // total > 2, floor(4 * 0.75) = 3
    }

    // Test multiple combinations
    public void testParseMultipleCombinationsLow() {
        int result = translator.calculateRequiredMatches("3<-1 5<50%", 3, false);
        assertEquals(3, result); // total <= 3, match all
    }

    public void testParseMultipleCombinationsMid() {
        int result = translator.calculateRequiredMatches("3<-1 5<50%", 4, false);
        assertEquals(3, result); // 3 < 4 <= 5, so -1 = 4 - 1 = 3
    }

    public void testParseMultipleCombinationsHigh() {
        int result = translator.calculateRequiredMatches("3<-1 5<50%", 6, false);
        assertEquals(3, result); // total > 5, floor(6 * 0.5) = 3
    }

    // Test default behavior
    public void testDefaultWithoutMust() {
        int result = translator.calculateRequiredMatches(null, 3, false);
        assertEquals(1, result); // No must clause, at least 1 should match
    }

    public void testDefaultWithMust() {
        int result = translator.calculateRequiredMatches(null, 3, true);
        assertEquals(0, result); // Has must clause, should is optional
    }
}
