/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action that coordinates PPL query execution using the BindableRel pipeline.
 * Obtains the current ClusterState from ClusterService and delegates to UnifiedQueryService.
 *
 * <p>On success, calls {@code listener.onResponse()} with the {@link UnifiedPPLResponse}.
 * On failure, calls {@code listener.onFailure()} with the exception.
 * Exactly one of onResponse or onFailure is called per request.
 */
public class TransportUnifiedPPLAction extends HandledTransportAction<UnifiedPPLRequest, UnifiedPPLResponse> {

    private static final Logger logger = LogManager.getLogger(TransportUnifiedPPLAction.class);

    private final ClusterService clusterService;
    private final UnifiedQueryService unifiedQueryService;

    @Inject
    public TransportUnifiedPPLAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        UnifiedQueryService unifiedQueryService
    ) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, UnifiedPPLRequest::new);
        this.clusterService = clusterService;
        this.unifiedQueryService = unifiedQueryService;
    }

    @Override
    protected void doExecute(Task task, UnifiedPPLRequest request, ActionListener<UnifiedPPLResponse> listener) {
        try {
            UnifiedPPLResponse response = unifiedQueryService.execute(request.getPplText(), clusterService.state());
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[UNIFIED_PPL] execution failed", e);
            listener.onFailure(e);
        }
    }
}
