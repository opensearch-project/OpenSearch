/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;

public class PublishCheckpointAction extends ActionType<RefreshResponse> {
    public static final PublishCheckpointAction INSTANCE = new PublishCheckpointAction();
    public static final String NAME = "indices:admin/publishCheckpoint";

    private PublishCheckpointAction() {super(NAME, RefreshResponse::new);}
}
