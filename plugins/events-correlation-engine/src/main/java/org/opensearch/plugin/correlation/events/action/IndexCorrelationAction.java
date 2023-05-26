/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.action.ActionType;

/**
 * Transport Action for indexing correlations
 *
 * @opensearch.internal
 */
public class IndexCorrelationAction extends ActionType<IndexCorrelationResponse> {

    /**
     * Instance of IndexCorrelationAction
     */
    public static final IndexCorrelationAction INSTANCE = new IndexCorrelationAction();
    /**
     * Name of IndexCorrelationAction
     */
    public static final String NAME = "cluster:admin/index/correlation/events";

    private IndexCorrelationAction() {
        super(NAME, IndexCorrelationResponse::new);
    }
}
