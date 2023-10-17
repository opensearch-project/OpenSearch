/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

public class SegmentReplicationRejectionStats implements Writeable, ToXContentFragment {

    /**
     * Total rejections due to segment replication backpressure
     */
    private long totalRejectionCount;

    public SegmentReplicationRejectionStats(final long totalRejectionCount) {
        this.totalRejectionCount = totalRejectionCount;
    }

    public SegmentReplicationRejectionStats(StreamInput in) throws IOException {
        // TODO: change to V_2_12_0 on main after backport to 2.x
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.totalRejectionCount = in.readVLong();
        }
    }

    public long getTotalRejectionCount() {
        return totalRejectionCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("segment_replication_backpressure");
        builder.field("total_rejected_requests", totalRejectionCount);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: change to V_2_12_0 on main after backport to 2.x
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeVLong(totalRejectionCount);
        }
    }

    @Override
    public String toString() {
        return "SegmentReplicationRejectionStats{ totalRejectedRequestCount=" + totalRejectionCount + '}';
    }

}
