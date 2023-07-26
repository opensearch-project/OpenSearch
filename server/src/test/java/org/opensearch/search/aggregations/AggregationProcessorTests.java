/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.mockito.ArgumentMatchers;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregator;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.test.TestSearchContext;

import java.util.ArrayList;
import java.util.Collection;

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
        testPreProcessCommon(agg, expectedGlobalAggs, expectedNonGlobalAggs, new ArrayList<>(), new ArrayList<>());
    }

    private void testPreProcessCommon(
        String agg,
        int expectedGlobalAggs,
        int expectedNonGlobalAggs,
        Collection<Collector> createdNonGlobalCollectors,
        Collection<Collector> createdGlobalCollectors
    ) throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(agg);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        testAggregationProcessor.preProcess(context);
        CollectorManager<?, ?> globalCollectorManager = null;
        CollectorManager<?, ?> nonGlobalCollectorManager = null;
        if (expectedNonGlobalAggs == 0 && expectedGlobalAggs == 0) {
            assertTrue(context.queryCollectorManagers().isEmpty());
            return;
        } else if (expectedGlobalAggs > 0 && expectedNonGlobalAggs > 0) {
            assertTrue(context.queryCollectorManagers().containsKey(NonGlobalAggCollectorManager.class));
            assertTrue(context.queryCollectorManagers().containsKey(GlobalAggCollectorManager.class));
            globalCollectorManager = context.queryCollectorManagers().get(GlobalAggCollectorManager.class);
            nonGlobalCollectorManager = context.queryCollectorManagers().get(NonGlobalAggCollectorManager.class);
        } else if (expectedGlobalAggs == 0) {
            assertTrue(context.queryCollectorManagers().containsKey(NonGlobalAggCollectorManager.class));
            assertFalse(context.queryCollectorManagers().containsKey(GlobalAggCollectorManager.class));
            nonGlobalCollectorManager = context.queryCollectorManagers().get(NonGlobalAggCollectorManager.class);
        } else {
            assertTrue(context.queryCollectorManagers().containsKey(GlobalAggCollectorManager.class));
            assertFalse(context.queryCollectorManagers().containsKey(NonGlobalAggCollectorManager.class));
            globalCollectorManager = context.queryCollectorManagers().get(GlobalAggCollectorManager.class);
        }

        Collector aggCollector;
        if (expectedGlobalAggs == 1) {
            aggCollector = globalCollectorManager.newCollector();
            createdGlobalCollectors.add(aggCollector);
            assertTrue(aggCollector instanceof BucketCollector);
            assertTrue(aggCollector instanceof GlobalAggregator);
        } else if (expectedGlobalAggs > 1) {
            aggCollector = globalCollectorManager.newCollector();
            createdGlobalCollectors.add(aggCollector);
            assertTrue(aggCollector instanceof MultiBucketCollector);
            for (Collector currentCollector : ((MultiBucketCollector) aggCollector).getCollectors()) {
                assertTrue(currentCollector instanceof GlobalAggregator);
            }
        }

        if (expectedNonGlobalAggs == 1) {
            aggCollector = nonGlobalCollectorManager.newCollector();
            createdNonGlobalCollectors.add(aggCollector);
            assertTrue(aggCollector instanceof BucketCollector);
            assertFalse(aggCollector instanceof GlobalAggregator);
        } else if (expectedNonGlobalAggs > 1) {
            aggCollector = nonGlobalCollectorManager.newCollector();
            createdNonGlobalCollectors.add(aggCollector);
            assertTrue(aggCollector instanceof MultiBucketCollector);
            for (Collector currentCollector : ((MultiBucketCollector) aggCollector).getCollectors()) {
                assertFalse(currentCollector instanceof GlobalAggregator);
            }
        }
    }

    private void testPostProcessCommon(String aggs, int numSlices, int expectedGlobalAggs, int expectedNonGlobalAggsPerSlice)
        throws Exception {
        final Collection<Collector> nonGlobalCollectors = new ArrayList<>();
        final Collection<Collector> globalCollectors = new ArrayList<>();
        testPreProcessCommon(aggs, expectedGlobalAggs, expectedNonGlobalAggsPerSlice, nonGlobalCollectors, globalCollectors);
        // newCollector is initialized once in the collector manager constructor
        for (int i = 1; i < numSlices; ++i) {
            if (expectedNonGlobalAggsPerSlice > 0) {
                nonGlobalCollectors.add(context.queryCollectorManagers().get(NonGlobalAggCollectorManager.class).newCollector());
            }
            if (expectedGlobalAggs > 0) {
                globalCollectors.add(context.queryCollectorManagers().get(GlobalAggCollectorManager.class).newCollector());
            }
        }
        final ContextIndexSearcher testSearcher = mock(ContextIndexSearcher.class);
        final IndexSearcher.LeafSlice[] slicesToReturn = new IndexSearcher.LeafSlice[numSlices];
        when(testSearcher.getSlices()).thenReturn(slicesToReturn);
        ((TestSearchContext) context).setSearcher(testSearcher);
        AggregationCollectorManager collectorManager;
        if (expectedNonGlobalAggsPerSlice > 0) {
            collectorManager = (AggregationCollectorManager) context.queryCollectorManagers().get(NonGlobalAggCollectorManager.class);
            collectorManager.reduce(nonGlobalCollectors).reduce(context.queryResult());
        }
        if (expectedGlobalAggs > 0) {
            collectorManager = (AggregationCollectorManager) context.queryCollectorManagers().get(GlobalAggCollectorManager.class);
            ReduceableSearchResult result = collectorManager.reduce(globalCollectors);
            when(testSearcher.search(nullable(Query.class), ArgumentMatchers.<CollectorManager<?, ReduceableSearchResult>>any()))
                .thenReturn(result);
        }
        assertTrue(context.queryResult().hasAggs());
        testAggregationProcessor.postProcess(context);
        assertTrue(context.queryResult().hasAggs());
        // for global aggs verify that search.search is called with CollectionManager
        if (expectedGlobalAggs > 0) {
            verify(testSearcher, times(1)).search(nullable(Query.class), ArgumentMatchers.<CollectorManager<?, ?>>any());
        }
        // after shard level reduce it should have only 1 InternalAggregation instance for each agg in request and internal aggregation
        // will be equal to sum of expected global and nonglobal aggs
        assertEquals(expectedNonGlobalAggsPerSlice + expectedGlobalAggs, context.queryResult().aggregations().expand().aggregations.size());
        assertNull(context.aggregations());
        assertTrue(context.queryCollectorManagers().isEmpty());
    }
}
