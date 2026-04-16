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
 * {@link ActionType} singleton for the analytics shard-level execution action.
 * Pairs the action name with the {@link ScanResponse} deserializer
 * for registration in {@code getActions()}.
 */
public class AnalyticsShardAction extends ActionType<ScanResponse> {

    /** Action name registered with the transport layer. */
    public static final String NAME = "indices:data/read/analytics/shard";

    /** Singleton instance. */
    public static final AnalyticsShardAction INSTANCE = new AnalyticsShardAction();

    private AnalyticsShardAction() {
        super(NAME, ScanResponse::new);
    }
}
