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

/**
 * Coordinator-level action for executing analytics queries. Registered in
 * {@link org.opensearch.analytics.AnalyticsPlugin#getActions()} with
 * {@link DefaultPlanExecutor} as the handler.
 *
 * <p>The action name {@code indices:data/read/analytics/query} is index-level,
 * matching the standard {@code read} action group pattern ({@code indices:data/read*}).
 * This ensures the security plugin's {@code SecurityFilter} evaluates index-level
 * permissions when the request is dispatched through {@code NodeClient.execute()}.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryAction extends ActionType<AnalyticsQueryResponse> {

    public static final String NAME = "indices:data/read/analytics/query";
    public static final AnalyticsQueryAction INSTANCE = new AnalyticsQueryAction();

    private AnalyticsQueryAction() {
        super(NAME, AnalyticsQueryResponse::new);
    }
}
