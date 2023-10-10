/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for Segment Replication Backpressure
 *
 * @opensearch.internal
 */
public class SegmentReplicationPressureStats implements Writeable, ToXContentFragment {
    private final long totalRejections;

    public SegmentReplicationPressureStats(long totalRejections) {
        this.totalRejections = totalRejections;
    }

    public SegmentReplicationPressureStats(StreamInput in) throws IOException {
        totalRejections = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalRejections);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("segment_replication_pressure");
        builder.field("total_rejections", totalRejections);
        builder.endObject(); // segment_replication_pressure

        return builder;
    }
}
