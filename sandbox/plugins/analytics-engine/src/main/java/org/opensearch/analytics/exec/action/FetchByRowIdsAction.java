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
 * Transport action for QTF fetch phase: fetches specific rows by global row ID.
 */
public class FetchByRowIdsAction extends ActionType<FetchByRowIdsResponse> {

    public static final String NAME = "indices:data/read/analytics/fetch_by_row_ids";
    public static final FetchByRowIdsAction INSTANCE = new FetchByRowIdsAction();

    private FetchByRowIdsAction() {
        super(NAME, FetchByRowIdsResponse::new);
    }
}
