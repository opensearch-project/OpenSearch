/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.action.ActionType;

/**
 * Transport action for cluster/node level top queries information.
 *
 * @opensearch.internal
 */
public class TopQueriesAction extends ActionType<TopQueriesResponse> {

    /**
     * The TopQueriesAction Instance.
     */
    public static final TopQueriesAction INSTANCE = new TopQueriesAction();
    /**
     * The name of this Action
     */
    public static final String NAME = "cluster:admin/opensearch/insights/top_queries";

    private TopQueriesAction() {
        super(NAME, TopQueriesResponse::new);
    }
}
