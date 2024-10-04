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
 * Transport action for obtaining Workload Management Stats.
 *
 * @opensearch.experimental
 */
<<<<<<< HEAD:server/src/main/java/org/opensearch/action/admin/cluster/wlm/QueryGroupStatsAction.java
public class QueryGroupStatsAction extends ActionType<QueryGroupStatsResponse> {
    public static final QueryGroupStatsAction INSTANCE = new QueryGroupStatsAction();
    public static final String NAME = "cluster:monitor/wlm/stats";
=======
public class WlmStatsAction extends ActionType<WlmStatsResponse> {
    public static final WlmStatsAction INSTANCE = new WlmStatsAction();
    public static final String NAME = "cluster:monitor/query_group_stats";
>>>>>>> bb4288b3eba (modify based on comments):server/src/main/java/org/opensearch/action/admin/cluster/wlm/WlmStatsAction.java

    private WlmStatsAction() {
        super(NAME, WlmStatsResponse::new);
    }
}
