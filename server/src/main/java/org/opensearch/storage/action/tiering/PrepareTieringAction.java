/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.broadcast.BroadcastResponse;

/**
 * Action type for pre-tiering sync operations on DFA indices.
 * This action flushes, refreshes, and waits for remote store sync on primary shards
 * before tiering can proceed.
 *
 * @opensearch.internal
 */
public class PrepareTieringAction extends ActionType<BroadcastResponse> {

    /**
     * Singleton instance of the PrepareTieringAction.
     */
    public static final PrepareTieringAction INSTANCE = new PrepareTieringAction();

    /**
     * Internal action name for pre-tiering sync operations.
     */
    public static final String NAME = "internal:admin/indices/prepare_tiering";

    private PrepareTieringAction() {
        super(NAME, BroadcastResponse::new);
    }
}
