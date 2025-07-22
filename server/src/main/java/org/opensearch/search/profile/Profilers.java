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

package org.opensearch.search.profile;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.profile.aggregation.AggregationProfiler;
import org.opensearch.search.profile.aggregation.ConcurrentAggregationProfiler;
import org.opensearch.search.profile.query.ConcurrentQueryProfileTree;
import org.opensearch.search.profile.query.ConcurrentQueryProfiler;
import org.opensearch.search.profile.query.InternalQueryProfileTree;
import org.opensearch.search.profile.query.QueryProfiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Wrapper around all the profilers that makes management easier.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class Profilers {

    private final ContextIndexSearcher searcher;
    private final List<QueryProfiler> queryProfilers;
    private final AggregationProfiler aggProfiler;
    private final boolean isConcurrentSegmentSearchEnabled;
    private final Function<Query, Collection<Supplier<ProfileMetric>>> customPluginMetrics;

    /** This {@link Profilers} instance will initially wrap one {@link QueryProfiler}. */
    public Profilers(ContextIndexSearcher searcher, boolean isConcurrentSegmentSearchEnabled) {
        this(searcher, isConcurrentSegmentSearchEnabled, query -> Collections.emptyList());
    }

    /** This {@link Profilers} instance will initially wrap one {@link QueryProfiler}. */
    public Profilers(
        ContextIndexSearcher searcher,
        boolean isConcurrentSegmentSearchEnabled,
        Function<Query, Collection<Supplier<ProfileMetric>>> customPluginMetrics
    ) {
        this.searcher = searcher;
        this.isConcurrentSegmentSearchEnabled = isConcurrentSegmentSearchEnabled;
        this.queryProfilers = new ArrayList<>();
        this.aggProfiler = isConcurrentSegmentSearchEnabled ? new ConcurrentAggregationProfiler() : new AggregationProfiler();
        this.customPluginMetrics = customPluginMetrics;
        addQueryProfiler();
    }

    /** Switch to a new profile. */
    public QueryProfiler addQueryProfiler() {
        QueryProfiler profiler = isConcurrentSegmentSearchEnabled
            ? new ConcurrentQueryProfiler(new ConcurrentQueryProfileTree(customPluginMetrics), customPluginMetrics)
            : new QueryProfiler(new InternalQueryProfileTree(customPluginMetrics));
        searcher.setProfiler(profiler);
        queryProfilers.add(profiler);
        return profiler;
    }

    /** Get the current profiler. */
    public QueryProfiler getCurrentQueryProfiler() {
        return queryProfilers.getLast();
    }

    /** Return the list of all created {@link QueryProfiler}s so far. */
    public List<QueryProfiler> getQueryProfilers() {
        return Collections.unmodifiableList(queryProfilers);
    }

    /** Return the {@link AggregationProfiler}. */
    public AggregationProfiler getAggregationProfiler() {
        return aggProfiler;
    }

}
