/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionType;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.core.action.ActionResponse;

/**
 * Coordinator-level action for executing analytics queries.
 *
 * <p>Currently used as a Guice injection vehicle for {@link DefaultPlanExecutor}
 * — the transport action registration lets Guice construct the executor with all
 * its dependencies ({@code TransportService}, {@code ClusterService}, etc.).
 * Front-end plugins invoke the executor directly via
 * {@link QueryPlanExecutor#execute}, not through transport.
 *
 * <p>Future: the transport path ({@code doExecute}) will accept query strings
 * for remote invocation.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryAction extends ActionType<ActionResponse> {

    public static final String NAME = "indices:data/read/analytics/query";
    public static final AnalyticsQueryAction INSTANCE = new AnalyticsQueryAction();

    private AnalyticsQueryAction() {
        super(NAME, in -> { throw new UnsupportedOperationException("Transport path not implemented yet"); });
    }
}
