/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Releases this data node's hash-shuffle buffers for a terminated query
 * ({@link ShuffleBufferManager#clearForQuery}). The coordinator broadcasts this to every data node
 * when a query reaches a terminal state, so buffers are reclaimed regardless of how the query ended
 * — including the FAILURE path (e.g. shuffle byte-budget breach), which does not cancel data-node
 * tasks and so never fires the per-task cancellation-listener cleanup.
 *
 * <p>Idempotent: a no-op on nodes that hold no buffers for the query.
 *
 * @opensearch.internal
 */
public class TransportAnalyticsClearShuffleAction extends HandledTransportAction<
    AnalyticsClearShuffleRequest,
    AnalyticsClearShuffleResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAnalyticsClearShuffleAction.class);

    private final ShuffleBufferManager shuffleBufferManager;

    @Inject
    public TransportAnalyticsClearShuffleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ShuffleBufferManager shuffleBufferManager
    ) {
        super(AnalyticsClearShuffleAction.NAME, transportService, actionFilters, AnalyticsClearShuffleRequest::new);
        this.shuffleBufferManager = shuffleBufferManager;
    }

    @Override
    protected void doExecute(Task task, AnalyticsClearShuffleRequest request, ActionListener<AnalyticsClearShuffleResponse> listener) {
        try {
            int removed = shuffleBufferManager.clearForQuery(request.getQueryId());
            if (removed > 0) {
                logger.debug("[ClearShuffle] released {} buffer(s) for query {}", removed, request.getQueryId());
            }
            listener.onResponse(new AnalyticsClearShuffleResponse(removed));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
