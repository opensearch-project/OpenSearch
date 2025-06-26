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

import java.util.List;
import java.util.Map;

/**
 * A {@link AbstractProfileBreakdown} for query timings with contexts.
 */
@PublicApi(since = "3.0.0")
public abstract class ContextualProfileBreakdown extends AbstractProfileBreakdown {

    /**
     * Sole constructor.
     *
     * @param metrics
     */
    public ContextualProfileBreakdown(Map<String, Class<? extends ProfileMetric>> metrics) {
        super(metrics);
    }

    public abstract ContextualProfileBreakdown context(Object context);

    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {}

    public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorToLeaves) {}
}
