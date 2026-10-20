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
 * Transport action for delivering a partition of hash-shuffle data from a producer data node to a
 * worker. Ported from OLAP's {@code ShuffleDataAction} pattern.
 *
 * @opensearch.internal
 */
public class AnalyticsShuffleDataAction extends ActionType<AnalyticsShuffleDataResponse> {

    public static final String NAME = "indices:data/read/analytics/shuffle";
    public static final AnalyticsShuffleDataAction INSTANCE = new AnalyticsShuffleDataAction();

    private AnalyticsShuffleDataAction() {
        super(NAME, AnalyticsShuffleDataResponse::new);
    }
}
