/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.mockito.ArgumentMatchers;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.test.TestSearchContext;

import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AggregationProcessorTests extends AggregationSetupTests {
    private final AggregationProcessor testAggregationProcessor = new ConcurrentAggregationProcessor();

    public void testPreProcessWithNoAggregations() {
        testAggregationProcessor.preProcess(context);
        assertTrue(context.queryCollectorManagers().isEmpty());
    }

    public void testPreProcessWithOnlyGlobalAggregator() throws Exception {
        testPreProcessCommon(globalAgg, 1, 0);
    }

    public void testPreProcessWithGlobalAndNonGlobalAggregators() throws Exception {
        testPreProcessCommon(globalNonGlobalAggs, 1, 1);
    }

    public void testPreProcessWithOnlyNonGlobalAggregators() throws Exception {
        testPreProcessCommon(multipleNonGlobalAggs, 0, 2);
    }

    public void testPostProcessWithNonGlobalAggregatorsAndSingleSlice() throws Exception {
        testPostProcessCommon(multipleNonGlobalAggs, 1, 0, 2);
    }

    public void testPostProcessWithNonGlobalAggregatorsAndMultipleSlices() throws Exception {
        testPostProcessCommon(multipleNonGlobalAggs, randomIntBetween(2, 5), 0, 2);
    }

    public void testPostProcessGlobalAndNonGlobalAggregators() throws Exception {
        testPostProcessCommon(globalNonGlobalAggs, randomIntBetween(2, 5), 1, 1);
    }

    private void testPreProcessCommon(String agg, int expectedGlobalAggs, int expectedNonGlobalAggs) throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(agg);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        testAggregationProcessor.preProcess(context);
        if (expectedNonGlobalAggs == 0) {
            assertTrue(context.queryCollectorManagers().isEmpty());
        }
        assertEquals(expectedGlobalAggs, context.aggregations().getGlobalAggregators().size());
        // non-global collectors are created only when newCollector is called
        assertTrue(context.aggregations().getNonGlobalAggregators().isEmpty());
        if (expectedNonGlobalAggs == 0) {
            assertTrue(context.queryCollectorManagers().isEmpty());
        } else {
            assertFalse(context.queryCollectorManagers().isEmpty());
        }
    }

    private void testPostProcessCommon(String aggs, int numSlices, int expectedGlobalAggs, int expectedNonGlobalAggsPerSlice)
        throws Exception {
        testPreProcessCommon(aggs, expectedGlobalAggs, expectedNonGlobalAggsPerSlice);
        // call newCollector once to initialize the aggregator for single slice
        for (int i = 0; i < numSlices; ++i) {
            context.queryCollectorManagers().get(AggregationProcessor.class).newCollector();
        }
        assertEquals(expectedNonGlobalAggsPerSlice * numSlices, context.aggregations().getNonGlobalAggregators().size());
        final ContextIndexSearcher testSearcher = mock(ContextIndexSearcher.class);
        final IndexSearcher.LeafSlice[] slicesToReturn = new IndexSearcher.LeafSlice[numSlices];
        when(testSearcher.getSlices()).thenReturn(slicesToReturn);
        ((TestSearchContext) context).setSearcher(testSearcher);
        testAggregationProcessor.postProcess(context);
        assertTrue(context.queryResult().hasAggs());
        // after shard level reduce it should have only 1 InternalAggregation instance for each agg in request
        if (expectedGlobalAggs > 0) {
            verify(testSearcher, times(1)).search(nullable(Query.class), ArgumentMatchers.any(Collector.class));
        }
        assertEquals(expectedNonGlobalAggsPerSlice + expectedGlobalAggs, context.queryResult().aggregations().expand().aggregations.size());
        assertNull(context.aggregations());
        assertTrue(context.queryCollectorManagers().isEmpty());
    }
}
