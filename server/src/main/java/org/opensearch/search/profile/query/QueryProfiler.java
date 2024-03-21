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

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.profile.AbstractProfiler;
import org.opensearch.search.profile.ContextualProfileBreakdown;

import java.util.Objects;

/**
 * This class acts as a thread-local storage for profiling a query.  It also
 * builds a representation of the query tree which is built constructed
 * "online" as the weights are wrapped by ContextIndexSearcher.  This allows us
 * to know the relationship between nodes in tree without explicitly
 * walking the tree or pre-wrapping everything
 * <p>
 * A Profiler is associated with every Search, not per Search-Request. E.g. a
 * request may execute two searches (query + global agg).  A Profiler just
 * represents one of those
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class QueryProfiler extends AbstractProfiler<ContextualProfileBreakdown<QueryTimingType>, Query> {

    /**
     * The root Collector used in the search
     */
    private InternalProfileComponent collector;

    public QueryProfiler(AbstractQueryProfileTree profileTree) {
        super(profileTree);
    }

    /** Set the collector that is associated with this profiler. */
    public void setCollector(InternalProfileComponent collector) {
        if (this.collector != null) {
            throw new IllegalStateException("The collector can only be set once.");
        }
        this.collector = Objects.requireNonNull(collector);
    }

    /**
     * Begin timing the rewrite phase of a request.  All rewrites are accumulated together into a
     * single metric
     */
    public void startRewriteTime() {
        ((AbstractQueryProfileTree) profileTree).startRewriteTime();
    }

    /**
     * Stop recording the current rewrite and add it's time to the total tally, returning the
     * cumulative time so far.
     */
    public void stopAndAddRewriteTime() {
        ((AbstractQueryProfileTree) profileTree).stopAndAddRewriteTime();
    }

    /**
     * The rewriting process is complex and hard to display because queries can undergo significant changes.
     * Instead of showing intermediate results, we display the cumulative time for the non-concurrent search case.
     * @return total time taken to rewrite all queries in this profile
     */
    public long getRewriteTime() {
        return ((AbstractQueryProfileTree) profileTree).getRewriteTime();
    }

    /**
     * Return the current root Collector for this search
     */
    public CollectorResult getCollector() {
        return collector.getCollectorTree();
    }

}
