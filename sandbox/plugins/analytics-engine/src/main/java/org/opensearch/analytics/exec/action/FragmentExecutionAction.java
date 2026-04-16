/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionType;
import org.opensearch.analytics.backend.ScanResponse;

/**
 * {@link ActionType} singleton for the analytics scan transport action.
 * Pairs the action name with the {@link ScanResponse} deserializer.
 * <p>
 * This is the typed replacement for the scan path previously handled by
 * {@link AnalyticsShardAction} with a generic {@code FragmentExecutionResponse}.
 */
public class FragmentExecutionAction extends ActionType<ScanResponse> {

    /** Action name registered with the transport layer. */
    public static final String NAME = "indices:data/read/analytics/scan";

    /** Singleton instance. */
    public static final AnalyticsScanAction INSTANCE = new FragmentExecutionAction();

    private FragmentExecutionAction() {
        super(NAME, ScanResponse::new);
    }
}
