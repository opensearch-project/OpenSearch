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
        decider.evaluateForQuery(query, null);
        assertTrue(decider.shouldUseIntraSegmentSearch());
    }

    public void testQueryDoesNotSupportIntraSegment() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(false);
        when(query.getName()).thenReturn("test_query");
        decider.evaluateForQuery(query, null);
        assertFalse(decider.shouldUseIntraSegmentSearch());
        assertTrue(decider.getReason().contains("test_query"));
    }

    public void testAggregationsSupportsIntraSegment() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForAggregations(aggs, null);
        assertTrue(decider.shouldUseIntraSegmentSearch());
    }

    public void testAggregationsDoesNotSupportIntraSegment() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(false);
        decider.evaluateForAggregations(aggs, null);
        assertFalse(decider.shouldUseIntraSegmentSearch());
    }

    public void testQueryNoVetoesAggregationYes() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(false);
        when(query.getName()).thenReturn("test_query");
        decider.evaluateForQuery(query, null);
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForAggregations(aggs, null);
        assertFalse(decider.shouldUseIntraSegmentSearch());
    }

    public void testQueryYesAggregationNo() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForQuery(query, null);
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(false);
        decider.evaluateForAggregations(aggs, null);
        assertFalse(decider.shouldUseIntraSegmentSearch());
    }

    public void testBothSupport() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        QueryBuilder query = mock(QueryBuilder.class);
        when(query.supportsIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForQuery(query, null);
        AggregatorFactories aggs = mock(AggregatorFactories.class);
        when(aggs.allFactoriesSupportIntraSegmentSearch()).thenReturn(true);
        decider.evaluateForAggregations(aggs, null);
        assertTrue(decider.shouldUseIntraSegmentSearch());
    }

    public void testNoQueryNoAggregations() {
        IntraSegmentSearchDecider decider = new IntraSegmentSearchDecider();
        assertFalse(decider.shouldUseIntraSegmentSearch());
    }
}
