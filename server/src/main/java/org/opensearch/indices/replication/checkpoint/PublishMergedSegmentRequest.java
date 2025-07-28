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
 * Replication request responsible for publishing merged segment request to a replica shard.
 *
 * @opensearch.internal
 */
public class PublishMergedSegmentRequest extends ReplicationRequest<PublishMergedSegmentRequest> {
    private final MergedSegmentCheckpoint mergedSegment;

    public PublishMergedSegmentRequest(MergedSegmentCheckpoint mergedSegment) {
        super(mergedSegment.getShardId());
        this.mergedSegment = mergedSegment;
    }

    public PublishMergedSegmentRequest(StreamInput in) throws IOException {
        super(in);
        this.mergedSegment = new MergedSegmentCheckpoint(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        mergedSegment.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PublishMergedSegmentRequest that)) return false;
        return Objects.equals(mergedSegment, that.mergedSegment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mergedSegment);
    }

    @Override
    public String toString() {
        return "PublishMergedSegmentRequest{" + "mergedSegment=" + mergedSegment + '}';
    }

    public MergedSegmentCheckpoint getMergedSegment() {
        return mergedSegment;
    }
}
