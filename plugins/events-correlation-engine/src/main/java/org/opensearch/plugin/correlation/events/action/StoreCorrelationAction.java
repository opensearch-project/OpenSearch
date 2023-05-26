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
 * Transport Action for storing correlations
 *
 * @opensearch.internal
 */
public class StoreCorrelationAction extends ActionType<StoreCorrelationResponse> {

    /**
     * Instance of StoreCorrelationAction
     */
    public static final StoreCorrelationAction INSTANCE = new StoreCorrelationAction();
    /**
     * Name of StoreCorrelationAction
     */
    public static final String NAME = "cluster:admin/store/correlation/events";

    private StoreCorrelationAction() {
        super(NAME, StoreCorrelationResponse::new);
    }
}
