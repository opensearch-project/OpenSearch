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

import java.util.List;
import java.util.Map;

/**
 * Provide contextual profile breakdowns which are associated with freestyle context. Used when concurrent
 * search over segments is activated and each collector needs own non-shareable profile breakdown instance.
 *
 * @opensearch.internal
 */
public abstract class ContextualProfileBreakdown<T extends Enum<T>> extends AbstractProfileBreakdown<T> {
    public ContextualProfileBreakdown(Class<T> clazz) {
        super(clazz);
    }

    /**
     * Return (or create) contextual profile breakdown instance
     * @param context freestyle context
     * @return contextual profile breakdown instance
     */
    public abstract AbstractProfileBreakdown<T> context(Object context);

    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {}

    public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorToLeaves) {}
}
