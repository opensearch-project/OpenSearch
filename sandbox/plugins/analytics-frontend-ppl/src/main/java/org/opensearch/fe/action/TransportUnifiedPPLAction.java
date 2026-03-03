/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.spi.SchemaProvider;
import org.opensearch.analytics.backend.EngineCapabilities;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.fe.planner.DefaultPlanExecutor;
import org.opensearch.fe.planner.PlanExecutor;
import org.opensearch.fe.planner.PushDownPlanner;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action that coordinates PPL query execution using the BindableRel pipeline.
 * Obtains the current ClusterState from ClusterService and delegates to UnifiedQueryService.
 *
 * <p>Receives {@link QueryPlanExecutor} and {@link SchemaProvider} via Guice injection
 * (the coordinator returns them as components) and assembles the pipeline locally.
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
        QueryPlanExecutor queryPlanExecutor,
        SchemaProvider schemaProvider
    ) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, UnifiedPPLRequest::new);
        this.clusterService = clusterService;

        PlanExecutor planExecutor = new DefaultPlanExecutor(queryPlanExecutor);
        EngineCapabilities engineCapabilities = EngineCapabilities.defaultCapabilities();
        PushDownPlanner pushDownPlanner = new PushDownPlanner(engineCapabilities, planExecutor);
        this.unifiedQueryService = new UnifiedQueryService(pushDownPlanner, schemaProvider);
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
