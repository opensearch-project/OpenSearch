/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action that coordinates PPL query execution.
 *
 * <p>Receives {@link EngineContext} and {@link QueryPlanExecutor} from the analytics-engine
 * plugin via Guice injection (enabled by {@code extendedPlugins = ['analytics-engine']}).
 */
public class TestPPLTransportAction extends HandledTransportAction<PPLRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(TestPPLTransportAction.class);

    private final UnifiedQueryService unifiedQueryService;

    @Inject
    public TestPPLTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContext engineContext,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor
    ) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, PPLRequest::new);
        this.unifiedQueryService = new UnifiedQueryService(executor, engineContext);
    }

    /** Test-only constructor that accepts a pre-built {@link UnifiedQueryService}. */
    public TestPPLTransportAction(TransportService transportService, ActionFilters actionFilters, UnifiedQueryService unifiedQueryService) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, PPLRequest::new);
        this.unifiedQueryService = unifiedQueryService;
    }

    @Override
    protected void doExecute(Task task, PPLRequest request, ActionListener<PPLResponse> listener) {
        try {
            PPLResponse response = unifiedQueryService.execute(request.getPplText());
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[UNIFIED_PPL] execution failed", e);
            listener.onFailure(e);
        }
    }
}
