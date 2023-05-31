/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.search.internal.SearchContext;
import org.opensearch.tracing.Level;
import org.opensearch.tracing.TracerManager;

/**
 * Replication group for a shard. Used by a primary shard to coordinate replication and recoveries.
 *
 * @opensearch.internal
 */
public class SearchOperationListenerTracerImpl implements SearchOperationListener {

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        SearchOperationListener.super.onPreQueryPhase(searchContext);
        TracerManager.getTracer().startSpan("onQueryPhase", Level.INFO);
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        SearchOperationListener.super.onQueryPhase(searchContext, tookInNanos);
        TracerManager.getTracer().endSpan();
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        SearchOperationListener.super.onPreFetchPhase(searchContext);
        TracerManager.getTracer().startSpan("onFetchPhase", Level.INFO);
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        SearchOperationListener.super.onFetchPhase(searchContext, tookInNanos);
        TracerManager.getTracer().endSpan();

    }
}
