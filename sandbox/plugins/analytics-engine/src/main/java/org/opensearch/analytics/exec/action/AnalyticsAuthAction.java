/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionType;

/**
 * Lightweight action used to check index-level authorization before analytics query execution.
 * The action name matches the {@code indices:data/read*} pattern so the security plugin's
 * standard {@code read} action group grants access.
 *
 * <p>The transport handler is a no-op — if the request reaches {@code doExecute}, it means
 * the security filter has already authorized the indices.
 */
public class AnalyticsAuthAction extends ActionType<AnalyticsAuthResponse> {

    public static final String NAME = "indices:data/read/analytics/authorize";
    public static final AnalyticsAuthAction INSTANCE = new AnalyticsAuthAction();

    private AnalyticsAuthAction() {
        super(NAME, AnalyticsAuthResponse::new);
    }
}
