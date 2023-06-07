/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.opensearch.search.query.EarlyTerminatingListener;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Collector Manager for internal agg profiling
 *
 * @opensearch.internal
 */
public class InternalProfileCollectorManager
    implements
        ProfileCollectorManager<InternalProfileCollector, ReduceableSearchResult>,
        EarlyTerminatingListener {
    private final CollectorManager<? extends Collector, ReduceableSearchResult> manager;
    private final String reason;
    private final List<InternalProfileCollectorManager> children;
    private long time = 0;
    private long reduceTime = 0;
    private long maxSliceEndTime = Long.MIN_VALUE;
    private long minSliceStartTime = Long.MAX_VALUE;
    private long maxSliceTime = 0;
    private long minSliceTime = Long.MAX_VALUE;
    private long avgSliceTime = 0;
    private int sliceCount = 0;

    public InternalProfileCollectorManager(
        CollectorManager<? extends Collector, ReduceableSearchResult> manager,
        String reason,
        List<InternalProfileCollectorManager> children
    ) {
        this.manager = manager;
        this.reason = reason;
        this.children = children;
    }

    @Override
    public InternalProfileCollector newCollector() throws IOException {
        return new InternalProfileCollector(manager.newCollector(), reason, children);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ReduceableSearchResult reduce(Collection<InternalProfileCollector> collectors) throws IOException {
        final long reduceStart = System.nanoTime();
        try {
            final Collection<Collector> subs = new ArrayList<>();

            for (final InternalProfileCollector collector : collectors) {
                subs.add(collector.getCollector());
                maxSliceEndTime = Math.max(maxSliceEndTime, collector.getSliceStartTime() + collector.getTime());
                minSliceStartTime = Math.min(minSliceStartTime, collector.getSliceStartTime());
                maxSliceTime = Math.max(maxSliceTime, collector.getTime());
                minSliceTime = Math.min(minSliceTime, collector.getTime());
                avgSliceTime += collector.getTime();
            }
            time = maxSliceEndTime - minSliceStartTime;
            sliceCount = collectors.size();
            avgSliceTime = sliceCount == 0 ? 0 : avgSliceTime / sliceCount;

            return ((CollectorManager<Collector, ReduceableSearchResult>) manager).reduce(subs);
        } finally {
            reduceTime = Math.max(1, System.nanoTime() - reduceStart);
        }

    }

    @Override
    public String getReason() {
        return reason;
    }

    @Override
    public long getTime() {
        return time;
    }

    public long getReduceTime() {
        return reduceTime;
    }

    public long getMaxSliceTime() {
        return maxSliceTime;
    }

    public long getMinSliceTime() {
        return minSliceTime;
    }

    public long getAvgSliceTime() {
        return avgSliceTime;
    }

    public int getSliceCount() {
        return sliceCount;
    }

    @Override
    public Collection<? extends InternalProfileComponent> children() {
        return children;
    }

    @Override
    public String getName() {
        return manager.getClass().getSimpleName();
    }

    @Override
    public CollectorResult getCollectorTree() {
        return doGetCollectorManagerTree(this);
    }

    static CollectorResult doGetCollectorManagerTree(InternalProfileCollectorManager collector) {
        List<CollectorResult> childResults = new ArrayList<>(collector.children().size());
        for (InternalProfileComponent child : collector.children()) {
            CollectorResult result = doGetCollectorManagerTree((InternalProfileCollectorManager) child);
            childResults.add(result);
        }
        return new CollectorResult(
            collector.getName(),
            collector.getReason(),
            collector.getTime(),
            collector.getReduceTime(),
            collector.getMaxSliceTime(),
            collector.getMinSliceTime(),
            collector.getAvgSliceTime(),
            collector.getSliceCount(),
            childResults
        );
    }

    @Override
    public void onEarlyTermination(int maxCountHits, boolean forcedTermination) {
        if (manager instanceof EarlyTerminatingListener) {
            ((EarlyTerminatingListener) manager).onEarlyTermination(maxCountHits, forcedTermination);
        }
    }
}
