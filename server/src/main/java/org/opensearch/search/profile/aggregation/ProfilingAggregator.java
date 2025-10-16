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
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.AggregationPath.PathElement;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.Timer;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.streaming.Streamable;
import org.opensearch.search.streaming.StreamingCostMetrics;

import java.io.IOException;
import java.util.Iterator;

/**
 * An aggregator that aggregates the performance profiling of other aggregations
 */
public class ProfilingAggregator extends Aggregator implements Streamable {

    private final Aggregator delegate;
    private final AggregationProfiler profiler;
    private AggregationProfileBreakdown profileBreakdown;

    public ProfilingAggregator(Aggregator delegate, AggregationProfiler profiler) {
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
    public void reset() {
        delegate.reset();
        super.reset();
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
    public String toString() {
        return delegate.toString();
    }

    @Override
    public StreamingCostMetrics getStreamingCostMetrics() {
        return delegate instanceof Streamable ? ((Streamable) delegate).getStreamingCostMetrics() : StreamingCostMetrics.nonStreamable();
    }

    @Override
    public Aggregator unwrapAggregator() {
        return delegate;
    }
}
