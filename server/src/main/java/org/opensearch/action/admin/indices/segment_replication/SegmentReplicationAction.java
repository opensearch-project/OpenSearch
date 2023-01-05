/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segment_replication;

import org.opensearch.action.ActionType;

public class SegmentReplicationAction extends ActionType<SegmentReplicationResponse> {
    public static final SegmentReplicationAction INSTANCE = new SegmentReplicationAction();
    public static final String NAME = "indices:monitor/segment_replication";

    private SegmentReplicationAction() {
        super(NAME, SegmentReplicationResponse::new);
    }
}
