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
 * Shares the {@code indices:data/read/analytics/*} prefix so the existing
 * security permission grant covers it.
 *
 * @opensearch.internal
 */
public final class AnalyticsCanMatchAction {

    public static final String NAME = "indices:data/read/analytics/can_match";

    private AnalyticsCanMatchAction() {}
}
