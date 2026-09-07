/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.opensearch.common.annotation.PublicApi;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Provide contextual profile breakdowns which are associated with freestyle context. Used when concurrent
 * search over segments is activated and each collector needs own non-shareable profile breakdown instance.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.2.0")
public abstract class ContextualProfileBreakdown extends AbstractProfileBreakdown {

    public ContextualProfileBreakdown(Collection<Supplier<ProfileMetric>> metrics) {
        super(metrics);
    }

    /**
     * Return (or create) contextual profile breakdown instance
     * @param context freestyle context
     * @return contextual profile breakdown instance
     */
    public abstract AbstractProfileBreakdown context(Object context);

    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {}

    /**
     * Associate a collector (slice) with a leaf it searched, along with the doc-id range
     * {@code [minDocId, maxDocId)} it was searched over. Enables additive per-slice/per-partition
     * profiling to report doc-ranges (including intra-segment partitions). Defaults to ignoring the
     * range and delegating to {@link #associateCollectorToLeaves(Collector, LeafReaderContext)}.
     */
    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf, int minDocId, int maxDocId) {
        associateCollectorToLeaves(collector, leaf);
    }

    public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorToLeaves) {}
}
