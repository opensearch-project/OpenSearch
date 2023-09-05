/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregator;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class AggregationCollectorManagerTests extends AggregationSetupTests {

    public void testNonGlobalCollectorManagers() throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(multipleNonGlobalAggs);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        int expectedAggCount = 2;
        final AggregationCollectorManager testAggCollectorManager = new NonGlobalAggCollectorManagerWithSingleCollector(context);
        Collector aggCollector = testAggCollectorManager.newCollector();
        assertTrue(aggCollector instanceof MultiBucketCollector);
        assertEquals(expectedAggCount, ((MultiBucketCollector) aggCollector).getCollectors().length);
        testCollectorManagerCommon(testAggCollectorManager);

        // test NonGlobalCollectorManager which will be used in concurrent segment search case
        testCollectorManagerCommon(new NonGlobalAggCollectorManager(context));
    }

    public void testGlobalCollectorManagers() throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(globalAgg);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        final AggregationCollectorManager testAggCollectorManager = new GlobalAggCollectorManagerWithSingleCollector(context);
        testCollectorManagerCommon(testAggCollectorManager);
        Collector aggCollector = testAggCollectorManager.newCollector();
        assertTrue(aggCollector instanceof BucketCollector);

        // test GlobalAggCollectorManager which will be used in concurrent segment search case
        testCollectorManagerCommon(new GlobalAggCollectorManager(context));
    }

    public void testAggCollectorManagersWithBothGlobalNonGlobalAggregators() throws Exception {
        final AggregatorFactories aggregatorFactories = getAggregationFactories(globalNonGlobalAggs);
        final SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        final AggregationCollectorManager testAggCollectorManager = new NonGlobalAggCollectorManagerWithSingleCollector(context);
        Collector aggCollector = testAggCollectorManager.newCollector();
        assertTrue(aggCollector instanceof BucketCollector);
        assertFalse(aggCollector instanceof GlobalAggregator);

        final AggregationCollectorManager testGlobalAggCollectorManager = new GlobalAggCollectorManagerWithSingleCollector(context);
        Collector globalAggCollector = testGlobalAggCollectorManager.newCollector();
        assertTrue(globalAggCollector instanceof BucketCollector);
        assertTrue(globalAggCollector instanceof GlobalAggregator);

        testCollectorManagerCommon(testAggCollectorManager);
        testCollectorManagerCommon(testGlobalAggCollectorManager);
    }

    public void testAssertionWhenCollectorManagerCreatesNoOPCollector() throws Exception {
        AggregatorFactories aggregatorFactories = getAggregationFactories(globalAgg);
        SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        expectThrows(AssertionError.class, () -> new NonGlobalAggCollectorManagerWithSingleCollector(context));
        expectThrows(AssertionError.class, () -> new NonGlobalAggCollectorManager(context));

        aggregatorFactories = getAggregationFactories(multipleNonGlobalAggs);
        contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        context.aggregations(contextAggregations);
        expectThrows(AssertionError.class, () -> new GlobalAggCollectorManagerWithSingleCollector(context));
        expectThrows(AssertionError.class, () -> new GlobalAggCollectorManager(context));
    }

    public void testAssertionInSingleCollectorCMReduce() throws Exception {
        AggregatorFactories aggregatorFactories = getAggregationFactories(globalNonGlobalAggs);
        SearchContextAggregations contextAggregations = new SearchContextAggregations(
            aggregatorFactories,
            mock(MultiBucketConsumerService.MultiBucketConsumer.class)
        );
        List<Collector> collectorsList = new ArrayList<>();
        collectorsList.add(mock(Collector.class));
        context.aggregations(contextAggregations);
        AggregationCollectorManager globalCM = new GlobalAggCollectorManagerWithSingleCollector(context);
        AggregationCollectorManager nonGlobalCM = new NonGlobalAggCollectorManagerWithSingleCollector(context);
        expectThrows(AssertionError.class, () -> globalCM.reduce(collectorsList));
        expectThrows(AssertionError.class, () -> nonGlobalCM.reduce(collectorsList));
    }

    private void testCollectorManagerCommon(AggregationCollectorManager collectorManager) throws Exception {
        final Collector expectedCollector = collectorManager.newCollector();
        for (int i = 0; i < randomIntBetween(2, 5); ++i) {
            final Collector newCollector = collectorManager.newCollector();
            if (collectorManager instanceof GlobalAggCollectorManagerWithSingleCollector
                || collectorManager instanceof NonGlobalAggCollectorManagerWithSingleCollector) {
                // calling the newCollector multiple times should return the same instance each time
                assertSame(expectedCollector, newCollector);
            } else if (collectorManager instanceof GlobalAggCollectorManager || collectorManager instanceof NonGlobalAggCollectorManager) {
                // calling the newCollector multiple times should not return the same instance each time
                assertNotSame(expectedCollector, newCollector);
            }
        }
    }
}
