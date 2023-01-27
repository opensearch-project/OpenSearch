/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Captures fail open stats per node. See {@link FailAwareWeightedRouting} for more details.
 *
 * @opensearch.internal
 */
public class FailOpenWeightedRoutingStats implements ToXContentFragment, Writeable {
    private AtomicInteger failOpenCount;

    public FailOpenWeightedRoutingStats() {
        failOpenCount = new AtomicInteger(0);
    }

    public FailOpenWeightedRoutingStats(StreamInput in) throws IOException {
        failOpenCount = new AtomicInteger(in.readInt());
    }

    public void updateFailOpenCount() {
        failOpenCount.getAndIncrement();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("fail_open");
        builder.startObject("stats");
        builder.field("fail_open_count", getFailOpenCount());
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public int getFailOpenCount() {
        return failOpenCount.get();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(failOpenCount.get());
    }
}
