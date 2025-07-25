/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Replication request responsible for publishing remote store merged segment request to a replica shard.
 *
 * @opensearch.internal
 */
public class RemoteStorePublishMergedSegmentRequest extends ReplicationRequest<RemoteStorePublishMergedSegmentRequest> {
    private final RemoteStoreMergedSegmentCheckpoint mergedSegment;

    public RemoteStorePublishMergedSegmentRequest(RemoteStoreMergedSegmentCheckpoint mergedSegment) {
        super(mergedSegment.getShardId());
        this.mergedSegment = mergedSegment;
    }

    public RemoteStorePublishMergedSegmentRequest(StreamInput in) throws IOException {
        super(in);
        this.mergedSegment = new RemoteStoreMergedSegmentCheckpoint(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        mergedSegment.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mergedSegment);
    }

    @Override
    public String toString() {
        return "RemoteStorePublishMergedSegmentRequest{mergedSegment=" + mergedSegment + '}';
    }

    public RemoteStoreMergedSegmentCheckpoint getMergedSegment() {
        return mergedSegment;
    }
}
