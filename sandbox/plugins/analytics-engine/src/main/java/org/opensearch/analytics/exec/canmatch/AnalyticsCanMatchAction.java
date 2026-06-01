/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

/**
 * Transport action name for the analytics can-match pre-filter phase.
 * Registered on the streaming transport service alongside the fragment handler.
 */
public final class AnalyticsCanMatchAction {

    // internal:* namespace because this is a coordinator→data-node action, not a user-facing
    // REST surface. Prevents accidental client invocation through the indices: permission grants.
    public static final String NAME = "internal:indices/analytics/can_match";

    private AnalyticsCanMatchAction() {}
}
