/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.opensearch.test.OpenSearchTestCase;

public class QueryNodeTypeTests extends OpenSearchTestCase {

    public void testIsComputationallyExpensive() {
        // Expensive types
        assertTrue(QueryNodeType.SCRIPT.isComputationallyExpensive());
        assertTrue(QueryNodeType.WILDCARD.isComputationallyExpensive());
        assertTrue(QueryNodeType.REGEXP.isComputationallyExpensive());
        assertTrue(QueryNodeType.FUZZY.isComputationallyExpensive());
        assertTrue(QueryNodeType.FUNCTION_SCORE.isComputationallyExpensive());
        assertTrue(QueryNodeType.VECTOR.isComputationallyExpensive());

        // Not expensive types
        assertFalse(QueryNodeType.TERM.isComputationallyExpensive());
        assertFalse(QueryNodeType.TERMS.isComputationallyExpensive());
        assertFalse(QueryNodeType.BOOLEAN.isComputationallyExpensive());
        assertFalse(QueryNodeType.MATCH_ALL.isComputationallyExpensive());
        assertFalse(QueryNodeType.RANGE.isComputationallyExpensive());
    }

    public void testPreferEarlyExecution() {
        // Prefer early execution
        assertTrue(QueryNodeType.TERM.preferEarlyExecution());
        assertTrue(QueryNodeType.TERMS.preferEarlyExecution());
        assertTrue(QueryNodeType.EXISTS.preferEarlyExecution());

        // Don't prefer early execution
        assertFalse(QueryNodeType.WILDCARD.preferEarlyExecution());
        assertFalse(QueryNodeType.SCRIPT.preferEarlyExecution());
        assertFalse(QueryNodeType.FUZZY.preferEarlyExecution());
        assertFalse(QueryNodeType.VECTOR.preferEarlyExecution());
    }
}
