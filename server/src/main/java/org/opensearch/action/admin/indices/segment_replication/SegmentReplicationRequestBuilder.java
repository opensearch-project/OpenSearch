/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segment_replication;

import org.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

/**
 * Segment Replication information request builder.
 *
 * @opensearch.internal
 */
public class SegmentReplicationRequestBuilder extends BroadcastOperationRequestBuilder<
    SegmentReplicationRequest,
    SegmentReplicationResponse,
    SegmentReplicationRequestBuilder> {

    public SegmentReplicationRequestBuilder(OpenSearchClient client, SegmentReplicationAction action) {
        super(client, action, new SegmentReplicationRequest());
    }

    public SegmentReplicationRequestBuilder setDetailed(boolean detailed) {
        request.detailed(detailed);
        return this;
    }

    public SegmentReplicationRequestBuilder setActiveOnly(boolean activeOnly) {
        request.activeOnly(activeOnly);
        return this;
    }

}
