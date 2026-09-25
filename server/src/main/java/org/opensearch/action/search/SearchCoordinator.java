/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.InternalSearchResponse;

/**
 * Coordinator interface for managing search phase transitions, error routing, and response delivery
 * across the lifecycle of a search request.
 *
 * @opensearch.internal
 */
@InternalApi
public interface SearchCoordinator {

    /**
     * Returns the currently executing search phase.
     */
    SearchPhase getCurrentPhase();

    /**
     * Builds and sends the final search response back to the user.
     *
     * @param internalSearchResponse the internal search response
     * @param queryResults           the results of the query phase
     */
    void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults);

    /**
     * Notifies the top-level listener of the provided exception.
     */
    void onFailure(Exception e);

    /**
     * Communicates a fatal phase failure back to the user. In contrast to a shard failure,
     * this method immediately fails the search request and returns the failure to the issuer of the request.
     *
     * @param phase the phase that failed
     * @param msg an optional message
     * @param cause the cause of the phase failure
     */
    void onPhaseFailure(SearchPhase phase, String msg, Throwable cause);

    /**
     * Records a shard failure for the given shard index. In contrast to a phase failure
     * ({@link #onPhaseFailure(SearchPhase, String, Throwable)}), this method will not immediately fail the request
     * but will record the shard failure. This should be called if a shard failure happens after we successfully
     * retrieved a result from that shard in a previous phase.
     */
    void onShardFailure(int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e);

    /**
     * Processes the phase transition from one phase to another. This method handles all errors that happen
     * during the initial run execution of the next phase. If there are no successful operations in the context
     * when this method is executed, the search is aborted and a response is returned to the user indicating
     * that all shards have failed.
     */
    void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase);

    /**
     * Sets the resource usage info for the current phase.
     */
    void setPhaseResourceUsages();
}
