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
 * {@link ActionType} singleton for the QTF (Query-Then-Fetch / late-materialization)
 * fetch-by-rowids action. Sibling of {@link FragmentExecutionAction} — different semantics
 * (no Substrait fragment, no plan conversion) so the dispatch table needs a distinct name,
 * but it shares the {@code indices:data/read/analytics/*} prefix so the existing security
 * permission grant covers both. Reuses {@link FragmentExecutionArrowResponse} as the
 * per-batch streamed response type.
 */
public class FetchByRowIdsAction extends ActionType<FragmentExecutionArrowResponse> {

    public static final String NAME = "indices:data/read/analytics/fetch_by_row_ids";

    public static final FetchByRowIdsAction INSTANCE = new FetchByRowIdsAction();

    private FetchByRowIdsAction() {
        super(NAME, FragmentExecutionArrowResponse::new);
    }
}
