/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.test.OpenSearchTestCase;

public class QueryCollectorArgumentsTests extends OpenSearchTestCase {

    public void testBuilder() {
        QueryCollectorArguments args = new QueryCollectorArguments.Builder().hasFilterCollector(true).build();

        assertTrue(args.hasFilterCollector());
    }

    public void testEquals() {
        QueryCollectorArguments args1 = new QueryCollectorArguments.Builder().hasFilterCollector(true).build();

        QueryCollectorArguments args2 = new QueryCollectorArguments.Builder().hasFilterCollector(true).build();

        QueryCollectorArguments args3 = new QueryCollectorArguments.Builder().hasFilterCollector(false).build();

        assertTrue(args1.equals(args2));          // Same values
        assertFalse(args1.equals(args3));         // Different values
        assertTrue(args1.equals(args1));          // Same object
    }

    public void testHashCode() {
        QueryCollectorArguments args1 = new QueryCollectorArguments.Builder().hasFilterCollector(true).build();

        QueryCollectorArguments args2 = new QueryCollectorArguments.Builder().hasFilterCollector(true).build();

        assertEquals(args1.hashCode(), args2.hashCode());
        assertEquals(args1.hashCode(), args1.hashCode());  // Consistent
    }

    public void testToString() {
        QueryCollectorArguments args = new QueryCollectorArguments.Builder().hasFilterCollector(true).build();

        String result = args.toString();

        assertEquals("QueryCollectorArguments[hasFilterCollector=true]", result);
    }
}
