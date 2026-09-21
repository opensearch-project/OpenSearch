/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class InternalRandomSamplerTests extends InternalSingleBucketAggregationTestCase<InternalRandomSampler> {

    /**
     * The shared test cases reduce instances together and expect doc counts to be summed, so those instances must not
     * scale anything. Scaling is covered by the cases below, which reduce explicitly.
     */
    @Override
    protected InternalRandomSampler createTestInstance(
        String name,
        long docCount,
        InternalAggregations aggregations,
        Map<String, Object> metadata
    ) {
        return new InternalRandomSampler(name, docCount, randomInt(), 1.0, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalRandomSampler reduced, List<InternalRandomSampler> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalRandomSampler::getSampledDocCount).sum(), reduced.getSampledDocCount());
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedRandomSampler.class;
    }

    public void testFinalReduceScalesDocCountAndSubAggregations() {
        InternalRandomSampler first = sampler(100L, 0.1, 20.0);
        InternalRandomSampler second = sampler(150L, 0.1, 30.0);

        InternalRandomSampler reduced = (InternalRandomSampler) first.reduce(Arrays.asList(first, second), finalReduceContext());

        assertEquals(2500L, reduced.getDocCount());
        assertEquals(250L, reduced.getSampledDocCount());
        InternalSum sum = reduced.getAggregations().get("sum");
        assertEquals(500.0, sum.getValue(), 1e-9);
    }

    public void testPartialReduceKeepsCountsRaw() {
        InternalRandomSampler first = sampler(100L, 0.1, 20.0);
        InternalRandomSampler second = sampler(150L, 0.1, 30.0);

        InternalRandomSampler reduced = (InternalRandomSampler) first.reduce(Arrays.asList(first, second), partialReduceContext());

        assertEquals(250L, reduced.getDocCount());
        assertEquals(250L, reduced.getSampledDocCount());
        InternalSum sum = reduced.getAggregations().get("sum");
        assertEquals(50.0, sum.getValue(), 1e-9);
    }

    public void testPartialThenFinalReduceScalesOnce() {
        InternalRandomSampler first = sampler(100L, 0.1, 20.0);
        InternalRandomSampler second = sampler(150L, 0.1, 30.0);
        InternalRandomSampler third = sampler(50L, 0.1, 10.0);

        InternalAggregation partial = first.reduce(Arrays.asList(first, second), partialReduceContext());
        InternalRandomSampler reduced = (InternalRandomSampler) partial.reduce(Arrays.asList(partial, third), finalReduceContext());

        assertEquals(3000L, reduced.getDocCount());
        assertEquals(300L, reduced.getSampledDocCount());
        InternalSum sum = reduced.getAggregations().get("sum");
        assertEquals(600.0, sum.getValue(), 1e-9);
    }

    public void testProbabilityOneScalesNothing() {
        InternalRandomSampler only = sampler(100L, 1.0, 20.0);

        InternalRandomSampler reduced = (InternalRandomSampler) only.reduce(Collections.singletonList(only), finalReduceContext());

        assertEquals(100L, reduced.getDocCount());
        InternalSum sum = reduced.getAggregations().get("sum");
        assertEquals(20.0, sum.getValue(), 1e-9);
    }

    public void testEqualsAndHashCodeCoverSamplingFields() {
        InternalAggregations subAggregations = InternalAggregations.from(Collections.emptyList());
        InternalRandomSampler base = new InternalRandomSampler("name", 10L, 1, 0.1, subAggregations, null);
        assertEquals(base, new InternalRandomSampler("name", 10L, 1, 0.1, subAggregations, null));
        assertNotEquals(base, new InternalRandomSampler("name", 10L, 2, 0.1, subAggregations, null));
        assertNotEquals(base, new InternalRandomSampler("name", 10L, 1, 0.2, subAggregations, null));
        assertNotEquals(base.hashCode(), new InternalRandomSampler("name", 10L, 1, 0.2, subAggregations, null).hashCode());
    }

    private static InternalRandomSampler sampler(long sampledDocCount, double probability, double sum) {
        InternalAggregations subAggregations = InternalAggregations.from(
            Collections.singletonList(new InternalSum("sum", sum, DocValueFormat.RAW, emptyMap()))
        );
        return new InternalRandomSampler("sampled", sampledDocCount, 42, probability, subAggregations, null);
    }

    private static InternalAggregation.ReduceContext finalReduceContext() {
        return InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays(),
            null,
            new MultiBucketConsumer(Integer.MAX_VALUE, new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)),
            PipelineTree.EMPTY
        );
    }

    private static InternalAggregation.ReduceContext partialReduceContext() {
        return InternalAggregation.ReduceContext.forPartialReduction(bigArrays(), null, () -> PipelineTree.EMPTY);
    }

    private static MockBigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(org.opensearch.common.settings.Settings.EMPTY), new NoneCircuitBreakerService());
    }
}
