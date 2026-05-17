/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.transport.Transport;

/**
 * Coordinator for managing the lifecycle of a search request across multiple phases.
 * <p>
 * This class provides a separate identity from the initial search phase ({@link AbstractSearchAsyncAction}),
 * breaking the circular dependency where the phase previously served as its own coordinator/context.
 * Downstream phases (fetch, expand, etc.) receive this coordinator as their {@link SearchPhaseContext},
 * rather than the initial phase itself.
 * <p>
 * Currently delegates all operations to the underlying {@link AbstractSearchAsyncAction}. As the refactoring
 * progresses, coordinator-specific state and logic will be moved here.
 *
 * @opensearch.internal
 */
@InternalApi
class SearchRequestCoordinator implements SearchPhaseContext {

    private final AbstractSearchAsyncAction<?> delegate;

    SearchRequestCoordinator(AbstractSearchAsyncAction<?> delegate) {
        this.delegate = delegate;
    }

    // --- Coordinator lifecycle methods ---

    /**
     * Entry point for the search request. Handles the zero-shard edge case,
     * then delegates to {@link #executePhase(SearchPhase)} for the initial phase.
     */
    void start(SearchPhase initialPhase) {
        if (delegate.getNumShards() == 0) {
            delegate.sendZeroShardResponse();
            return;
        }
        executePhase(initialPhase);
    }

    /**
     * Executes a search phase: fires lifecycle callbacks, runs the phase, and handles exceptions.
     * Called by {@link #start(SearchPhase)} for the initial phase and by
     * {@link #executeNextPhase(SearchPhase, SearchPhase)} for subsequent phases.
     */
    void executePhase(SearchPhase phase) {
        delegate.executePhase(phase);
    }

    // --- SearchCoordinator methods ---

    @Override
    public SearchPhase getCurrentPhase() {
        return delegate.getCurrentPhase();
    }

    @Override
    public void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        delegate.sendSearchResponse(internalSearchResponse, queryResults);
    }

    @Override
    public void onFailure(Exception e) {
        delegate.onFailure(e);
    }

    @Override
    public void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        delegate.onPhaseFailure(phase, msg, cause);
    }

    @Override
    public void onShardFailure(int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        delegate.onShardFailure(shardIndex, shardTarget, e);
    }

    @Override
    public void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {
        delegate.executeNextPhase(currentPhase, nextPhase);
    }

    @Override
    public void setPhaseResourceUsages() {
        delegate.setPhaseResourceUsages();
    }

    // --- SearchPhaseContext resource-access methods ---

    @Override
    public int getNumShards() {
        return delegate.getNumShards();
    }

    @Override
    public Logger getLogger() {
        return delegate.getLogger();
    }

    @Override
    public SearchTask getTask() {
        return delegate.getTask();
    }

    @Override
    public SearchRequest getRequest() {
        return delegate.getRequest();
    }

    @Override
    public Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return delegate.getConnection(clusterAlias, nodeId);
    }

    @Override
    public SearchTransportService getSearchTransport() {
        return delegate.getSearchTransport();
    }

    @Override
    public ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt) {
        return delegate.buildShardSearchRequest(shardIt);
    }

    @Override
    public void addReleasable(Releasable releasable) {
        delegate.addReleasable(releasable);
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(command);
    }
}
