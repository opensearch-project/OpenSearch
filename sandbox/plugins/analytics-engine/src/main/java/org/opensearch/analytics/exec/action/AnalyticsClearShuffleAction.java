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
 * Transport action the coordinator broadcasts to every data node when a query reaches a terminal
 * state (success / failure / cancel), instructing the node to release any hash-shuffle buffers it
 * holds for that query ({@code ShuffleBufferManager.clearForQuery}).
 *
 * <p>Needed because a query that FAILS (e.g. on the shuffle byte budget) does not cancel the
 * data-node tasks, so the per-task cancellation-listener cleanup never fires — leaving the buffered
 * Arrow-IPC {@code byte[]} on-heap until the next query OOMs. {@code clearForQuery} is idempotent
 * and a no-op on nodes that hold no buffers for the query, so a blanket broadcast to all data nodes
 * is correct and cheap (no need to track the exact participating set).
 *
 * @opensearch.internal
 */
public class AnalyticsClearShuffleAction extends ActionType<AnalyticsClearShuffleResponse> {

    public static final String NAME = "indices:data/read/analytics/shuffle/clear";
    public static final AnalyticsClearShuffleAction INSTANCE = new AnalyticsClearShuffleAction();

    private AnalyticsClearShuffleAction() {
        super(NAME, AnalyticsClearShuffleResponse::new);
    }
}
