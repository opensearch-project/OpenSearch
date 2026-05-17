/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.list;

import org.opensearch.action.ActionType;

/**
 * ActionType for listing snapshot indices with pagination.
 *
 * @opensearch.internal
 */
public class SnapshotIndicesListAction extends ActionType<SnapshotIndicesListResponse> {

    public static final SnapshotIndicesListAction INSTANCE = new SnapshotIndicesListAction();
    public static final String NAME = "cluster:admin/snapshot/list/indices";

    private SnapshotIndicesListAction() {
        super(NAME, SnapshotIndicesListResponse::new);
    }
}


