/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.test.OpenSearchTestCase;

public class SqlLikePatternTests extends OpenSearchTestCase {

    public void testPrefixShape() {
        assertEquals(SqlLikePattern.Shape.PREFIX, SqlLikePattern.classify("foo%"));
        assertEquals(SqlLikePattern.Shape.PREFIX, SqlLikePattern.classify("a%"));
        // escaped percent before the single trailing wildcard is still a prefix
        assertEquals(SqlLikePattern.Shape.PREFIX, SqlLikePattern.classify("fo\\%o%"));
    }

    public void testGeneralShapes() {
        assertEquals("leading wildcard", SqlLikePattern.Shape.GENERAL, SqlLikePattern.classify("%foo"));
        assertEquals("embedded wildcard", SqlLikePattern.Shape.GENERAL, SqlLikePattern.classify("f%o%"));
        assertEquals("contains-style", SqlLikePattern.Shape.GENERAL, SqlLikePattern.classify("%foo%"));
        assertEquals("underscore present", SqlLikePattern.Shape.GENERAL, SqlLikePattern.classify("fo_%"));
        assertEquals("single underscore", SqlLikePattern.Shape.GENERAL, SqlLikePattern.classify("on_"));
        assertEquals("no wildcard at all", SqlLikePattern.Shape.GENERAL, SqlLikePattern.classify("exact"));
        // fully escaped percent → no real wildcard → GENERAL (no prefix scan applies)
        assertEquals("escaped trailing percent is literal", SqlLikePattern.Shape.GENERAL, SqlLikePattern.classify("foo\\%"));
    }

    public void testPrefixLiteralResolvesEscapes() {
        assertEquals("foo", SqlLikePattern.prefixLiteral("foo%"));
        assertEquals("fo%o", SqlLikePattern.prefixLiteral("fo\\%o%"));
        assertEquals("a_b", SqlLikePattern.prefixLiteral("a\\_b%"));
    }
}
