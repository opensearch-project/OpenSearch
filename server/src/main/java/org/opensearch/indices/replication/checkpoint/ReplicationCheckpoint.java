/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a Replication Checkpoint which is sent to a replica shard.
 *
 * @opensearch.internal
 */
public class ReplicationCheckpoint implements Writeable {

    private final ShardId shardId;
    private final long primaryTerm;
    private final long segmentsGen;
    private final long seqNo;
    private final long segmentInfosVersion;

    public ReplicationCheckpoint(ShardId shardId, long primaryTerm, long segmentsGen, long seqNo, long segmentInfosVersion) {
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.segmentsGen = segmentsGen;
        this.seqNo = seqNo;
        this.segmentInfosVersion = segmentInfosVersion;
    }

    public ReplicationCheckpoint(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        primaryTerm = in.readLong();
        segmentsGen = in.readLong();
        seqNo = in.readLong();
        segmentInfosVersion = in.readLong();
    }

    /**
     * The primary term of this Replication Checkpoint.
     *
     * @return the primary term
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * @return the Segments Gen number
     */
    public long getSegmentsGen() {
        return segmentsGen;
    }

    /**
     * @return the Segment Info version
     */
    public long getSegmentInfosVersion() {
        return segmentInfosVersion;
    }

    /**
     * @return the Seq number
     */
    public long getSeqNo() {
        return seqNo;
    }

    /**
     * Shard Id of primary shard.
     *
     * @return the Shard Id
     */
    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeLong(primaryTerm);
        out.writeLong(segmentsGen);
        out.writeLong(seqNo);
        out.writeLong(segmentInfosVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationCheckpoint that = (ReplicationCheckpoint) o;
        return primaryTerm == that.primaryTerm
            && segmentsGen == that.segmentsGen
            && seqNo == that.seqNo
            && segmentInfosVersion == that.segmentInfosVersion
            && Objects.equals(shardId, that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, primaryTerm, segmentsGen, seqNo);
    }

    /**
     * Checks if other is aheadof current replication point by comparing segmentInfosVersion. Returns true for null
     */
    public boolean isAheadOf(@Nullable ReplicationCheckpoint other) {
        return other == null || segmentInfosVersion > other.getSegmentInfosVersion();
    }

    @Override
    public String toString() {
        return "ReplicationCheckpoint{"
            + "shardId="
            + shardId
            + ", primaryTerm="
            + primaryTerm
            + ", segmentsGen="
            + segmentsGen
            + ", seqNo="
            + seqNo
            + ", version="
            + segmentInfosVersion
            + '}';
    }
}
