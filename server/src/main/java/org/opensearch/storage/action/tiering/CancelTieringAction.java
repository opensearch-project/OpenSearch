/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Action for cancelling ongoing tiering operations.
 * This action can be used to cancel both hot-to-warm and warm-to-hot migrations
 * when shards get stuck in RUNNING_SHARD_RELOCATION state.
 */
public class CancelTieringAction extends ActionType<AcknowledgedResponse> {

    /** Singleton instance. */
    public static final CancelTieringAction INSTANCE = new CancelTieringAction();
    /** Action name for cancel tiering. */
    public static final String NAME = "indices:admin/_tier/cancel";

    private CancelTieringAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
