/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.profile.aggregation;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.AggregationPath.PathElement;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.Timer;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.streaming.Streamable;
import org.opensearch.search.streaming.StreamingCostMetrics;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * An aggregator that aggregates the performance profiling of other aggregations
 */
public class ProfilingAggregator extends Aggregator implements Streamable {

    private final Aggregator delegate;
    private final AggregationProfiler profiler;
    private AggregationProfileBreakdown profileBreakdown;

    public ProfilingAggregator(Aggregator delegate, AggregationProfiler profiler) throws IOException {
        this.profiler = profiler;
        this.delegate = delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public ScoreMode scoreMode() {
        return delegate.scoreMode();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SearchContext context() {
        return delegate.context();
    }

    @Override
    public Aggregator parent() {
        return delegate.parent();
    }

    @Override
    public Aggregator subAggregator(String name) {
        return delegate.subAggregator(name);
    }

    @Override
    public Aggregator resolveSortPath(PathElement next, Iterator<PathElement> path) {
        return delegate.resolveSortPath(next, path);
    }

    @Override
    public BucketComparator bucketComparator(String key, SortOrder order) {
        return delegate.bucketComparator(key, order);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        Timer timer = profileBreakdown.getTimer(AggregationTimingType.BUILD_AGGREGATION);
        timer.start();
        try {
            return delegate.buildAggregations(owningBucketOrds);
        } finally {
            timer.stop();
            delegate.collectDebugInfo(profileBreakdown::addDebugInfo);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return delegate.buildEmptyAggregation();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        Timer timer = profileBreakdown.getTimer(AggregationTimingType.BUILD_LEAF_COLLECTOR);
        timer.start();
        try {
            return new ProfilingLeafBucketCollector(delegate.getLeafCollector(ctx), profileBreakdown);
        } finally {
            timer.stop();
        }
    }

    @Override
    public void preCollection() throws IOException {
        this.profileBreakdown = profiler.getQueryBreakdown(delegate);
        Timer timer = profileBreakdown.getTimer(AggregationTimingType.INITIALIZE);
        timer.start();
        try {
            delegate.preCollection();
        } finally {
            timer.stop();
        }
        profiler.pollLastElement();
    }

    @Override
    public void postCollection() throws IOException {
        Timer timer = profileBreakdown.getTimer(AggregationTimingType.POST_COLLECTION);
        timer.start();
        try {
            delegate.postCollection();
        } finally {
            timer.stop();
        }
    }

    @Override
    public boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        Timer timer = profileBreakdown.getTimer(AggregationTimingType.PRE_COMPUTE);
        timer.start();
        try {
            // Here we can add logic to check if star tree precomputation is possible and do it accordingly
            // For now, we just call the super method
            return delegate.tryPrecomputeAggregationForLeaf(ctx);
        } finally {
            timer.stop();
        }
    }

    @Override
    public FixedBitSet scanStarTree(SearchContext context,
        ValuesSource.Numeric valuesSource,
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        String metric
    ) throws IOException {
        Timer timer = profileBreakdown.getTimer(StarTreeAggregationTimingType.SCAN_STAR_TREE_SEGMENTS);
        timer.start();
        try {
            return delegate.scanStarTree(context, valuesSource, ctx, starTree, metric);
        } finally {
            timer.stop();
        }
    }

    @Override
    public void buildBucketsFromStarTree(
        SearchContext context,
        ValuesSource.Numeric valuesSource,
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        String metric,
        Consumer<Long> valueConsumer,
        Runnable finalConsumer,
        FixedBitSet filteredValues
    ) throws IOException {
        Timer timer = profileBreakdown.getTimer(StarTreeAggregationTimingType.BUILD_BUCKETS_FROM_STAR_TREE);
        timer.start();
        try {
            delegate.buildBucketsFromStarTree(context, valuesSource, ctx, starTree, metric, valueConsumer, finalConsumer, filteredValues);
        } finally {
            timer.stop();
        }
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public static Aggregator unwrap(Aggregator agg) {
        if (agg instanceof ProfilingAggregator) {
            return ((ProfilingAggregator) agg).delegate;
        }
        return agg;
    }

    public Aggregator getDelegate() {
        return delegate;
    }

    @Override
    public StreamingCostMetrics getStreamingCostMetrics() {
        return delegate instanceof Streamable ? ((Streamable) delegate).getStreamingCostMetrics() : StreamingCostMetrics.nonStreamable();
    }
}
