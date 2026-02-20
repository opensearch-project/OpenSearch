/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.action;

import org.opensearch.action.ActionType;

/**
 * Action type for coordinator-level query execution.
 *
 * <p>Accepts serialized Substrait plan bytes, fans out to data nodes
 * via streaming transport, and returns aggregated results.
 */
public class QueryAction extends ActionType<QueryResponse> {

    public static final QueryAction INSTANCE = new QueryAction();
    public static final String NAME = "indices:data/read/dqe/query";

    private QueryAction() {
        super(NAME, QueryResponse::new);
    }
}
