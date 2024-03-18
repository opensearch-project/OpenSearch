/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * ReplicationStats is used to provide segment replication statistics at an index,
 * node and cluster level on a segment replication enabled cluster.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.10.0")
public class ReplicationStats implements ToXContentFragment, Writeable {

    public long maxBytesBehind;
    public long maxReplicationLag;
    public long totalBytesBehind;

    public ReplicationStats(long maxBytesBehind, long totalBytesBehind, long maxReplicationLag) {
        this.maxBytesBehind = maxBytesBehind;
        this.totalBytesBehind = totalBytesBehind;
        this.maxReplicationLag = maxReplicationLag;
    }

    public ReplicationStats(StreamInput in) throws IOException {
        this.maxBytesBehind = in.readVLong();
        this.totalBytesBehind = in.readVLong();
        this.maxReplicationLag = in.readVLong();
    }

    public ReplicationStats() {

    }

    public void add(ReplicationStats other) {
        if (other != null) {
            maxBytesBehind = Math.max(other.maxBytesBehind, maxBytesBehind);
            totalBytesBehind += other.totalBytesBehind;
            maxReplicationLag = Math.max(other.maxReplicationLag, maxReplicationLag);
        }
    }

    public long getMaxBytesBehind() {
        return this.maxBytesBehind;
    }

    public long getTotalBytesBehind() {
        return this.totalBytesBehind;
    }

    public long getMaxReplicationLag() {
        return this.maxReplicationLag;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(maxBytesBehind);
        out.writeVLong(totalBytesBehind);
        out.writeVLong(maxReplicationLag);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEGMENT_REPLICATION);
        builder.field(Fields.MAX_BYTES_BEHIND, maxBytesBehind);
        builder.field(Fields.TOTAL_BYTES_BEHIND, totalBytesBehind);
        builder.field(Fields.MAX_REPLICATION_LAG, maxReplicationLag);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for segment replication statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SEGMENT_REPLICATION = "segment_replication";
        static final String MAX_BYTES_BEHIND = "max_bytes_behind";
        static final String TOTAL_BYTES_BEHIND = "total_bytes_behind";
        static final String MAX_REPLICATION_LAG = "max_replication_lag";
    }
}
