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
    private final ReferencedSegmentsCheckpoint referencedSegments;

    public PublishReferencedSegmentsRequest(ReferencedSegmentsCheckpoint referencedSegments) {
        super(referencedSegments.getShardId());
        this.referencedSegments = referencedSegments;
    }

    public PublishReferencedSegmentsRequest(StreamInput in) throws IOException {
        super(in);
        this.referencedSegments = new ReferencedSegmentsCheckpoint(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        referencedSegments.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PublishReferencedSegmentsRequest that)) return false;
        return Objects.equals(referencedSegments, that.referencedSegments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referencedSegments);
    }

    @Override
    public String toString() {
        return "PublishReferencedSegmentRequest{" + "referencedSegments=" + referencedSegments + '}';
    }

    public ReferencedSegmentsCheckpoint getReferencedSegments() {
        return referencedSegments;
    }
}
