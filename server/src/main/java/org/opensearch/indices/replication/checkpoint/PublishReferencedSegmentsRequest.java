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
 * Replication request responsible for publishing referenced segments request to a replica shard.
 *
 * @opensearch.internal
 */
public class PublishReferencedSegmentsRequest extends ReplicationRequest<PublishReferencedSegmentsRequest> {
    private final ReferencedSegmentsCheckpoint referencedSegmentsCheckpoint;

    public PublishReferencedSegmentsRequest(ReferencedSegmentsCheckpoint referencedSegmentsCheckpoint) {
        super(referencedSegmentsCheckpoint.getShardId());
        this.referencedSegmentsCheckpoint = referencedSegmentsCheckpoint;
    }

    public PublishReferencedSegmentsRequest(StreamInput in) throws IOException {
        super(in);
        this.referencedSegmentsCheckpoint = new ReferencedSegmentsCheckpoint(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        referencedSegmentsCheckpoint.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PublishReferencedSegmentsRequest that)) return false;
        return Objects.equals(referencedSegmentsCheckpoint, that.referencedSegmentsCheckpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referencedSegmentsCheckpoint);
    }

    @Override
    public String toString() {
        return "PublishReferencedSegmentRequest{" + "referencedSegmentsCheckpoint=" + referencedSegmentsCheckpoint + '}';
    }

    public ReferencedSegmentsCheckpoint getReferencedSegmentsCheckpoint() {
        return referencedSegmentsCheckpoint;
    }
}
