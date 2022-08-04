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
        final Collection<Collector> subs = new ArrayList<>();

        for (final InternalProfileCollector collector : collectors) {
            subs.add(collector.getCollector());
            time += collector.getTime();
        }

        return ((CollectorManager<Collector, ReduceableSearchResult>) manager).reduce(subs);
    }

    @Override
    public String getReason() {
        return reason;
    }

    @Override
    public long getTime() {
        return time;
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
        return InternalProfileCollector.doGetCollectorTree(this);
    }

    @Override
    public void onEarlyTermination(int maxCountHits, boolean forcedTermination) {
        if (manager instanceof EarlyTerminatingListener) {
            ((EarlyTerminatingListener) manager).onEarlyTermination(maxCountHits, forcedTermination);
        }
    }
}
