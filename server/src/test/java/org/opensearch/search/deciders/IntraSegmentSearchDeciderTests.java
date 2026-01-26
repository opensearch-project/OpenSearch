/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.deciders;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IntraSegmentSearchDeciderTests extends OpenSearchTestCase {

    public void testQuerySupportsIntraSegment() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForQuery(query);
        assertTrue(decider.shouldUseIntraSegmentSearch());
        assertEquals("query/aggregations support intra-segment search", decider.getReason());
    }

    public void testQueryDoesNotSupportIntraSegment() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(false);
        when(query.getName()).thenReturn("test_query");
        decider.evaluateForQuery(query);
        assertFalse(decider.shouldUseIntraSegmentSearch());
        assertEquals("test_query does not support intra-segment search", decider.getReason());
    }

    public void testAggregationsSupportsIntraSegment() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForAggregations(aggs);
        assertTrue(decider.shouldUseIntraSegmentSearch());
        assertEquals("query/aggregations support intra-segment search", decider.getReason());
    }

    public void testAggregationsDoesNotSupportIntraSegment() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(false);
        decider.evaluateForAggregations(aggs);
        assertFalse(decider.shouldUseIntraSegmentSearch());
        assertEquals("some aggregations do not support intra-segment search", decider.getReason());
    }

    public void testQueryNoVetoesAggregationYes() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(false);
        when(query.getName()).thenReturn("test_query");
        decider.evaluateForQuery(query);
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForAggregations(aggs);
        assertFalse(decider.shouldUseIntraSegmentSearch());
        assertEquals("test_query does not support intra-segment search", decider.getReason());
    }

    public void testQueryYesAggregationNo() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForQuery(query);
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(false);
        decider.evaluateForAggregations(aggs);
        assertFalse(decider.shouldUseIntraSegmentSearch());
        assertEquals("some aggregations do not support intra-segment search", decider.getReason());
    }

    public void testBothSupport() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForQuery(query);
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForAggregations(aggs);
        assertTrue(decider.shouldUseIntraSegmentSearch());
        assertEquals("query/aggregations support intra-segment search", decider.getReason());
    }

    public void testNoQueryNoAggregations() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        assertFalse(decider.shouldUseIntraSegmentSearch());
        assertEquals("no query or aggregation evaluated", decider.getReason());
    }
}
