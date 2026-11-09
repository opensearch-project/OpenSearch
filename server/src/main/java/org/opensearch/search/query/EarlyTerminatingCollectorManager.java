/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Manager for the EarlyTerminatingCollector
 *
 * @opensearch.internal
 */
public class EarlyTerminatingCollectorManager<C extends Collector>
    implements
        CollectorManager<EarlyTerminatingCollector, ReduceableSearchResult>,
        EarlyTerminatingListener {

    private final CollectorManager<C, ReduceableSearchResult> manager;
    private final int maxCountHits;
    private boolean forceTermination;

    EarlyTerminatingCollectorManager(CollectorManager<C, ReduceableSearchResult> manager, int maxCountHits, boolean forceTermination) {
        this.manager = manager;
        this.maxCountHits = maxCountHits;
        this.forceTermination = forceTermination;
    }

    @Override
    public EarlyTerminatingCollector newCollector() throws IOException {
        return new EarlyTerminatingCollector(manager.newCollector(), maxCountHits, false /* forced termination is not supported */);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ReduceableSearchResult reduce(Collection<EarlyTerminatingCollector> collectors) throws IOException {
        final List<C> innerCollectors = new ArrayList<>(collectors.size());

        boolean didTerminateEarly = false;
        for (EarlyTerminatingCollector collector : collectors) {
            innerCollectors.add((C) collector.getCollector());
            if (collector.hasEarlyTerminated()) {
                didTerminateEarly = true;
            }
        }

        if (didTerminateEarly) {
            onEarlyTermination(maxCountHits, forceTermination);

            final ReduceableSearchResult result = manager.reduce(innerCollectors);
            return new ReduceableSearchResult() {
                @Override
                public void reduce(QuerySearchResult r) throws IOException {
                    result.reduce(r);
                    r.terminatedEarly(true);
                }
            };
        }

        return manager.reduce(innerCollectors);
    }

    @Override
    public void onEarlyTermination(int maxCountHits, boolean forcedTermination) {
        if (manager instanceof EarlyTerminatingListener earlyTerminatingListener) {
            earlyTerminatingListener.onEarlyTermination(maxCountHits, forcedTermination);
        }
    }
}
