/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.admin.cluster.insights.top_queries;

import org.opensearch.action.ActionType;

/**
 * Transport action for cluster/node level top queries information.
 *
 * @opensearch.internal
 */
public class TopQueriesAction extends ActionType<TopQueriesResponse> {

    public static final TopQueriesAction INSTANCE = new TopQueriesAction();
    public static final String NAME = "cluster:monitor/insights/top_queries";

    private TopQueriesAction() {
        super(NAME, TopQueriesResponse::new);
    }
}
