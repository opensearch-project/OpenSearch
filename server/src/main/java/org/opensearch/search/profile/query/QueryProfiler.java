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
import org.opensearch.search.profile.AbstractProfiler;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.Timer;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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
 * @opensearch.internal
 */
public final class QueryProfiler extends AbstractProfiler<ContextualProfileBreakdown<QueryTimingType>, Query> {

    /**
     * The root Collector used in the search
     */
    private InternalProfileComponent collector;

    public QueryProfiler(boolean concurrent) {
        super(concurrent ? new ConcurrentQueryProfileTree() : new InternalQueryProfileTree());
        if (concurrent) {
            long threadId = Thread.currentThread().getId();
            threadToProfileTree = new ConcurrentHashMap<>();
            threadToProfileTree.put(threadId, (ConcurrentQueryProfileTree) profileTree);
        }
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
     *
     * @return cumulative rewrite time
     */
    public long stopAndAddRewriteTime() {
        return ((AbstractQueryProfileTree) profileTree).stopAndAddRewriteTime();
    }

    /**
     * The rewriting process is complex and hard to display because queries can undergo significant changes.
     * Instead of showing intermediate results, we display the cumulative time for the non-concurrent search case.
     * In the concurrent search case, we compute the sum of the non-overlapping time across all slices and add
     * the rewrite time from the non-concurrent path.
     * @return total time taken to rewrite all queries in this profile
     */
    public long getRewriteTime() {
        long rewriteTime = ((AbstractQueryProfileTree) profileTree).getRewriteTime();
        if (profileTree instanceof ConcurrentQueryProfileTree) {
            List<Timer> timers = getConcurrentPathRewriteTimers();
            LinkedList<long[]> mergedIntervals = mergeRewriteTimeIntervals(timers);
            for (long[] interval : mergedIntervals) {
                rewriteTime += interval[1] - interval[0];
            }
        }
        return rewriteTime;
    }

    // package private for unit testing
    LinkedList<long[]> mergeRewriteTimeIntervals(List<Timer> timers) {
        LinkedList<long[]> mergedIntervals = new LinkedList<>();
        timers.sort(Comparator.comparingLong(Timer::getEarliestTimerStartTime));
        for (Timer timer : timers) {
            long startTime = timer.getEarliestTimerStartTime();
            long endTime = timer.getEarliestTimerStartTime() + timer.getApproximateTiming();
            if (mergedIntervals.isEmpty() || mergedIntervals.getLast()[1] < timer.getEarliestTimerStartTime()) {
                long[] interval = new long[2];
                interval[0] = startTime;
                interval[1] = endTime;
                mergedIntervals.add(interval);
            } else {
                mergedIntervals.getLast()[1] = Math.max(mergedIntervals.getLast()[1], endTime);
            }
        }
        return mergedIntervals;
    }

    /**
     * @return a list of concurrent path rewrite timers for this concurrent search
     */
    public List<Timer> getConcurrentPathRewriteTimers() {
        return ((ConcurrentQueryProfileTree) profileTree).getConcurrentPathRewriteTimers();
    }

    /**
     * @return the thread to profile tree map for this concurrent search
     */
    public Map<Long, ConcurrentQueryProfileTree> getThreadToProfileTree() {
        return threadToProfileTree;
    }

    /**
     * Return the current root Collector for this search
     */
    public CollectorResult getCollector() {
        return collector.getCollectorTree();
    }

}
