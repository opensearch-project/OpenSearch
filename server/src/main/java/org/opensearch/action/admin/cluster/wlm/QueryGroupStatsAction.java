/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.wlm;

import org.opensearch.action.ActionType;

/**
 * Transport action for obtaining QueryGroup Stats.
 *
 * @opensearch.experimental
 */
public class QueryGroupStatsAction extends ActionType<QueryGroupStatsResponse> {
    public static final QueryGroupStatsAction INSTANCE = new QueryGroupStatsAction();
    public static final String NAME = "cluster:monitor/wlm/stats";

    private QueryGroupStatsAction() {
        super(NAME, QueryGroupStatsResponse::new);
    }
}
