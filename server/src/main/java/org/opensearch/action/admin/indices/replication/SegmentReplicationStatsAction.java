/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.replication;

import java.util.List;
import org.opensearch.action.ActionScopes;
import org.opensearch.action.ActionType;
import org.opensearch.identity.Scope;

/**
 * Segment Replication stats information action
 *
 * @opensearch.internal
 */
public class SegmentReplicationStatsAction extends ActionType<SegmentReplicationStatsResponse> {
    public static final SegmentReplicationStatsAction INSTANCE = new SegmentReplicationStatsAction();
    public static final String NAME = "indices:monitor/segment_replication";

    private SegmentReplicationStatsAction() {
        super(NAME, SegmentReplicationStatsResponse::new);
    }

    @Override
    public List<Scope> allowedScopes() {
        return List.of(ActionScopes.Index_ALL);
    }
}
