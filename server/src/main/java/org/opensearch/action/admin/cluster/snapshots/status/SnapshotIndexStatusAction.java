/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.status;

import org.opensearch.action.ActionType;

/**
 * Action type for listing snapshot index statuses with pagination support.
 *
 * @opensearch.internal
 */
public class SnapshotIndexStatusAction extends ActionType<SnapshotIndexStatusResponse> {

    public static final SnapshotIndexStatusAction INSTANCE = new SnapshotIndexStatusAction();
    public static final String NAME = "cluster:admin/snapshot/list/indices";

    private SnapshotIndexStatusAction() {
        super(NAME, SnapshotIndexStatusResponse::new);
    }
}
