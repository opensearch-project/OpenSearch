/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.common.Randomness;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregation factory for the {@code random_sampler} aggregation.
 *
 * @opensearch.internal
 */
public class RandomSamplerAggregatorFactory extends AggregatorFactory {

    private final double probability;
    private final int seed;

    RandomSamplerAggregatorFactory(
        String name,
        double probability,
        Integer seed,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
        this.probability = probability;
        // One seed per shard request when the user did not pick one, so that repeated requests see different samples.
        // The factory is built once per shard request, so every slice of a concurrent search shares this seed.
        this.seed = seed == null ? Randomness.get().nextInt() : seed;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return new RandomSamplerAggregator(name, probability, seed, factories, searchContext, parent, cardinality, metadata);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        // Sampling is per segment and slices own disjoint segments, so slices cannot collect the same document twice.
        // Slice results are combined with a partial reduce, which leaves the scaling to the final reduce.
        return true;
    }
}
