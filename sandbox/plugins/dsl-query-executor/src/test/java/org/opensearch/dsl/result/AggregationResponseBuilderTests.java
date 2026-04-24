/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationResponseBuilderTests extends OpenSearchTestCase {

    public void testBuildEmptyAggregations() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());

        InternalAggregations aggs = builder.build(List.of());
        assertNotNull(aggs);
        assertEquals(0, aggs.asList().size());
    }

    public void testBuildMetricWithNoResults() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createMetricTranslator(AvgAggregationBuilder.class));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());
        AvgAggregationBuilder avgAgg = new AvgAggregationBuilder("avg_price").field("price");

        InternalAggregations aggs = builder.build(List.of(avgAgg));
        assertEquals(1, aggs.asList().size());
        assertEquals("avg_price", aggs.asList().get(0).getName());
    }

    public void testBuildBucketWithNoResults() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createBucketTranslator(TermsAggregationBuilder.class, "brand"));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_brand").field("brand");

        InternalAggregations aggs = builder.build(List.of(termsAgg));
        assertEquals(1, aggs.asList().size());
        assertEquals("by_brand", aggs.asList().get(0).getName());
    }

    public void testBuildMultipleAggregations() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createMetricTranslator(AvgAggregationBuilder.class));
        registry.register(createBucketTranslator(TermsAggregationBuilder.class, "brand"));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());

        AvgAggregationBuilder avgAgg = new AvgAggregationBuilder("avg_price").field("price");
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_brand").field("brand");

        InternalAggregations aggs = builder.build(List.of(avgAgg, termsAgg));
        assertEquals(2, aggs.asList().size());
    }

    public void testBuildNestedAggregation() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createBucketTranslator(TermsAggregationBuilder.class, "brand"));
        registry.register(createMetricTranslator(AvgAggregationBuilder.class));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());

        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_brand")
            .field("brand")
            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"));

        InternalAggregations aggs = builder.build(List.of(termsAgg));
        assertEquals(1, aggs.asList().size());
        assertEquals("by_brand", aggs.asList().get(0).getName());
    }

    @SuppressWarnings("unchecked")
    private <T extends AggregationBuilder> MetricTranslator<T> createMetricTranslator(Class<T> aggClass) {
        MetricTranslator<T> translator = mock(MetricTranslator.class);
        when(translator.getAggregationType()).thenReturn(aggClass);
        when(translator.toInternalAggregation(any(), any())).thenAnswer(inv -> {
            InternalAggregation agg = mock(InternalAggregation.class);
            when(agg.getName()).thenReturn(inv.getArgument(0));
            return agg;
        });
        return translator;
    }

    @SuppressWarnings("unchecked")
    private <T extends AggregationBuilder> BucketTranslator<T> createBucketTranslator(Class<T> aggClass, String fieldName) {
        BucketTranslator<T> translator = mock(BucketTranslator.class);
        when(translator.getAggregationType()).thenReturn(aggClass);

        GroupingInfo grouping = mock(GroupingInfo.class);
        when(grouping.getFieldNames()).thenReturn(List.of(fieldName));
        when(translator.getGrouping(any())).thenReturn(grouping);
        when(translator.getSubAggregations(any())).thenReturn(List.of());

        when(translator.toBucketAggregation(any(), any())).thenAnswer(inv -> {
            InternalAggregation agg = mock(InternalAggregation.class);
            when(agg.getName()).thenReturn(((AggregationBuilder) inv.getArgument(0)).getName());
            return agg;
        });
        return translator;
    }
}
