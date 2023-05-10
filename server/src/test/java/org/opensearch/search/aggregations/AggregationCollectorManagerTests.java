/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.opensearch.search.aggregations.bucket.terms.NumericTermsAggregator;

import java.util.Arrays;

import static org.mockito.Mockito.mock;

public class AggregationCollectorManagerTests extends AggregationSetupTests {

    public void testNewCollectorWithGlobalAndNonGlobalAggregators() throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(globalNonGlobalAggs);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        final AggregationCollectorManager testAggCollectorManager = new AggregationCollectorManager(context);
        Collector aggCollector = testAggCollectorManager.newCollector();
        assertTrue(context.aggregations().getGlobalAggregators().isEmpty());
        assertTrue(aggCollector instanceof NumericTermsAggregator);
        assertEquals(1, context.aggregations().getNonGlobalAggregators().size());
        assertEquals(context.aggregations().getNonGlobalAggregators().get(0), aggCollector);

        // again call the newCollector
        aggCollector = testAggCollectorManager.newCollector();
        assertTrue(context.aggregations().getGlobalAggregators().isEmpty());
        assertEquals(2, context.aggregations().getNonGlobalAggregators().size());
        assertNotEquals(context.aggregations().getNonGlobalAggregators().get(0), aggCollector);
        assertEquals(context.aggregations().getNonGlobalAggregators().get(1), aggCollector);
    }

    public void testMultipleNonGlobalAggregators() throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(multipleNonGlobalAggs);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        int expectedAggCount = 2;
        final AggregationCollectorManager testAggCollectorManager = new AggregationCollectorManager(context);
        Collector aggCollector = testAggCollectorManager.newCollector();
        assertTrue(context.aggregations().getGlobalAggregators().isEmpty());
        assertTrue(aggCollector instanceof MultiBucketCollector);
        assertEquals(expectedAggCount, context.aggregations().getNonGlobalAggregators().size());
        assertEquals(Arrays.toString(context.aggregations().getNonGlobalAggregators().toArray()), aggCollector.toString());

        // call the newCollector multiple times
        int numCollectors = randomIntBetween(2, 5);
        for (int i = 1; i < numCollectors; ++i) {
            aggCollector = testAggCollectorManager.newCollector();
            assertTrue(context.aggregations().getGlobalAggregators().isEmpty());
            assertTrue(aggCollector instanceof MultiBucketCollector);
        }
        assertEquals(numCollectors * expectedAggCount, context.aggregations().getNonGlobalAggregators().size());
    }

    public void testOnlyGlobalAggregators() throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(globalAgg);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        final AggregationCollectorManager testAggCollectorManager = new AggregationCollectorManager(context);
        expectThrows(AssertionError.class, testAggCollectorManager::newCollector);
    }
}
