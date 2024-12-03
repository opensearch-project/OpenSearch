/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for gRPC connections
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class GrpcStats implements Writeable, ToXContentFragment {
    private final long totalRequestCount;
    private final long totalActiveConnections;

    public GrpcStats(long totalRequestCount, long totalActiveConnections) {
        this.totalRequestCount = totalRequestCount;
        this.totalActiveConnections = totalActiveConnections;
    }

    public GrpcStats(StreamInput in) throws IOException {
        totalRequestCount = in.readVLong();
        totalActiveConnections = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalRequestCount);
        out.writeVLong(totalActiveConnections);
    }

    static final class Fields {
        static final String GRPC = "grpc";
        static final String CURRENT_OPEN = "current_open";
        static final String TOTAL_OPENED = "total_opened";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GrpcStats.Fields.GRPC);
        builder.field(GrpcStats.Fields.CURRENT_OPEN, totalActiveConnections);
        builder.field(GrpcStats.Fields.TOTAL_OPENED, totalRequestCount);
        builder.endObject();
        return builder;
    }
}
