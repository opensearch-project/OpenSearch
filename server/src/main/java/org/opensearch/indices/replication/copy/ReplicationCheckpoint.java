/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

public class ReplicationCheckpoint implements Writeable {

    private final ShardId shardId;
    private final long primaryTerm;
    private final long segmentsGen;
    private final long seqNo;

    public ReplicationCheckpoint(ShardId shardId, long primaryTerm, long segments_gen, long seqNo) {
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.segmentsGen = segments_gen;
        this.seqNo = seqNo;
    }

    public ReplicationCheckpoint(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        primaryTerm = in.readLong();
        segmentsGen = in.readLong();
        seqNo = in.readLong();
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getSegmentsGen() {
        return segmentsGen;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeLong(primaryTerm);
        out.writeLong(segmentsGen);
        out.writeLong(seqNo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationCheckpoint that = (ReplicationCheckpoint) o;
        return primaryTerm == that.primaryTerm
            && segmentsGen == that.segmentsGen
            && seqNo == that.seqNo
            && Objects.equals(shardId, that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, primaryTerm, segmentsGen, seqNo);
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
            + '}';
    }
}
