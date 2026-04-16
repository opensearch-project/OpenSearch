/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionType;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;

/**
 * {@link ActionType} singleton for the analytics scan transport action.
 * Pairs the action name with the {@link FragmentExecutionResponse} deserializer.
 * <p>
 * This is the typed replacement for the scan path previously handled by
 * {@link AnalyticsShardAction} with a generic {@code FragmentExecutionResponse}.
 */
public class FragmentExecutionAction extends ActionType<FragmentExecutionResponse> {

    /** Action name registered with the transport layer. */
    public static final String NAME = "indices:data/read/analytics/scan";

    /** Singleton instance. */
    public static final FragmentExecutionAction INSTANCE = new FragmentExecutionAction();

    private FragmentExecutionAction() {
        super(NAME, FragmentExecutionResponse::new);
    }
}
