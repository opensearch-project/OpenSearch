/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.opensearch.search.profile.AbstractProfileBreakdown;

import java.util.EmptyStackException;
import java.util.List;
import java.util.Map;

/**
 * Interface for profiling context
 */
public interface ProfileContext {
    /**
     * Return (or create) contextual profile breakdown instance
     * @param context freestyle context
     * @return contextual profile breakdown instance
     */
    public AbstractProfileBreakdown context(Object context);

    default public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {}

    default public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorToLeaves) {}
}
