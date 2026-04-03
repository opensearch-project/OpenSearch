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
import org.opensearch.analytics.AnalyticsEngineService;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action that coordinates PPL query execution.
 *
 * <p>Retrieves {@link EngineContext} and {@link QueryPlanExecutor} from
 * {@link AnalyticsEngineService} (set by analytics-engine plugin during startup).
 */
public class TestPPLTransportAction extends HandledTransportAction<PPLRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(TestPPLTransportAction.class);

    private final UnifiedQueryService unifiedQueryService;

    @Inject
    public TestPPLTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, PPLRequest::new);

        AnalyticsEngineService svc = AnalyticsEngineService.getInstance();
        if (svc != null) {
            QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = svc.getPlanExecutor();
            this.unifiedQueryService = new UnifiedQueryService(executor, svc.getEngineContext());
        } else {
            logger.warn("Analytics engine not available — PPL queries will fail");
            this.unifiedQueryService = null;
        }
    }

    /** Test-only constructor that accepts a pre-built {@link UnifiedQueryService}. */
    public TestPPLTransportAction(TransportService transportService, ActionFilters actionFilters, UnifiedQueryService unifiedQueryService) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, PPLRequest::new);
        this.unifiedQueryService = unifiedQueryService;
    }

    @Override
    protected void doExecute(Task task, PPLRequest request, ActionListener<PPLResponse> listener) {
        if (unifiedQueryService == null) {
            listener.onFailure(new IllegalStateException("Analytics engine is not installed"));
            return;
        }
        try {
            PPLResponse response = unifiedQueryService.execute(request.getPplText());
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[UNIFIED_PPL] execution failed", e);
            listener.onFailure(e);
        }
    }
}
