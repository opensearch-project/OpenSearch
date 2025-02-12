/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.Timer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class acts as a thread-local storage for profiling a query with concurrent execution
 *
 * @opensearch.internal
 */
public final class ConcurrentQueryProfiler extends QueryProfiler {

    private final Map<Long, ConcurrentQueryProfileTree> threadToProfileTree;
    // The LinkedList does not need to be thread safe, as the map associates thread IDs with LinkedList, and only
    // one thread will access the LinkedList at a time.
    private final Map<Long, LinkedList<Timer>> threadToRewriteTimers;

    public ConcurrentQueryProfiler(AbstractQueryProfileTree profileTree) {
        super(profileTree);
        long threadId = getCurrentThreadId();
        // We utilize LinkedHashMap to preserve the insertion order of the profiled queries
        threadToProfileTree = Collections.synchronizedMap(new LinkedHashMap<>());
        threadToProfileTree.put(threadId, (ConcurrentQueryProfileTree) profileTree);
        threadToRewriteTimers = new ConcurrentHashMap<>();
        threadToRewriteTimers.put(threadId, new LinkedList<>());
    }

    @Override
    public ContextualProfileBreakdown<QueryTimingType> getQueryBreakdown(Query query) {
        ConcurrentQueryProfileTree profileTree = threadToProfileTree.computeIfAbsent(
            getCurrentThreadId(),
            k -> new ConcurrentQueryProfileTree()
        );
        return profileTree.getProfileBreakdown(query);
    }

    /**
     * Removes the last (e.g. most recent) element on ConcurrentQueryProfileTree stack.
     */
    @Override
    public void pollLastElement() {
        ConcurrentQueryProfileTree concurrentProfileTree = threadToProfileTree.get(getCurrentThreadId());
        if (concurrentProfileTree != null) {
            concurrentProfileTree.pollLast();
        }
    }

    /**
     * @return a hierarchical representation of the profiled tree
     */
    @Override
    public List<ProfileResult> getTree() {
        List<ProfileResult> profileResults = new ArrayList<>();
        for (Map.Entry<Long, ConcurrentQueryProfileTree> profile : threadToProfileTree.entrySet()) {
            profileResults.addAll(profile.getValue().getTree());
        }
        return profileResults;
    }

    /**
     * Begin timing the rewrite phase of a request
     */
    @Override
    public void startRewriteTime() {
        Timer rewriteTimer = new Timer();
        threadToRewriteTimers.computeIfAbsent(getCurrentThreadId(), k -> new LinkedList<>()).add(rewriteTimer);
        rewriteTimer.start();
    }

    /**
     * Stop recording the current rewrite timer
     */
    public void stopAndAddRewriteTime() {
        Timer rewriteTimer = threadToRewriteTimers.get(getCurrentThreadId()).getLast();
        rewriteTimer.stop();
    }

    /**
     * @return total time taken to rewrite all queries in this concurrent query profiler
     */
    @Override
    public long getRewriteTime() {
        long totalRewriteTime = 0L;
        List<Timer> rewriteTimers = new LinkedList<>();
        threadToRewriteTimers.values().forEach(rewriteTimers::addAll);
        LinkedList<long[]> mergedIntervals = mergeRewriteTimeIntervals(rewriteTimers);
        for (long[] interval : mergedIntervals) {
            totalRewriteTime += interval[1] - interval[0];
        }
        return totalRewriteTime;
    }

    // package private for unit testing
    LinkedList<long[]> mergeRewriteTimeIntervals(List<Timer> timers) {
        LinkedList<long[]> mergedIntervals = new LinkedList<>();
        timers.sort(Comparator.comparingLong(Timer::getEarliestTimerStartTime));
        for (Timer timer : timers) {
            long startTime = timer.getEarliestTimerStartTime();
            long endTime = startTime + timer.getApproximateTiming();
            if (mergedIntervals.isEmpty() || mergedIntervals.getLast()[1] < startTime) {
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

    private long getCurrentThreadId() {
        return Thread.currentThread().getId();
    }
}
