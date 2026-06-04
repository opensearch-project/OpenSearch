/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a Pre-copy merged segment which is sent to a replica shard.
 * Inherit {@link ReplicationCheckpoint}, but the segmentsGen will not be used.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class MergedSegmentCheckpoint extends ReplicationCheckpoint {
    private final String segmentName;

    public MergedSegmentCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long segmentInfosVersion,
        long length,
        String codec,
        Map<String, StoreFileMetadata> metadataMap,
        String segmentName
    ) {
        super(shardId, primaryTerm, SequenceNumbers.NO_OPS_PERFORMED, segmentInfosVersion, length, codec, metadataMap);
        this.segmentName = segmentName;
    }

    public MergedSegmentCheckpoint(StreamInput in) throws IOException {
        super(in);
        segmentName = in.readString();
    }

    /**
     * The segmentName
     *
     * @return the segmentCommitInfo name
     */
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(segmentName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergedSegmentCheckpoint that = (MergedSegmentCheckpoint) o;
        return getPrimaryTerm() == that.getPrimaryTerm()
            && segmentName.equals(that.segmentName)
            && Objects.equals(getShardId(), that.getShardId())
            && getCodec().equals(that.getCodec());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getShardId(), getPrimaryTerm(), segmentName);
    }

    @Override
    public String toString() {
        return "MergedSegmentCheckpoint{"
            + "shardId="
            + getShardId()
            + ", primaryTerm="
            + getPrimaryTerm()
            + ", segmentsGen="
            + getSegmentsGen()
            + ", version="
            + getSegmentInfosVersion()
            + ", size="
            + getLength()
            + ", codec="
            + getCodec()
            + ", timestamp="
            + getCreatedTimeStamp()
            + ", segmentName="
            + segmentName
            + '}';
    }
}
