/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;

/**
 * Internal action for executing DSL queries through the analytics engine.
 * Accepts a {@link org.opensearch.action.search.SearchRequest} and returns a {@link SearchResponse}.
 */
public class DslExecuteAction extends ActionType<SearchResponse> {

    /** Action name registered with the transport layer. */
    public static final String NAME = "cluster:internal/dsl/execute";
    /** Singleton instance. */
    public static final DslExecuteAction INSTANCE = new DslExecuteAction();

    private DslExecuteAction() {
        super(NAME, SearchResponse::new);
    }
}
