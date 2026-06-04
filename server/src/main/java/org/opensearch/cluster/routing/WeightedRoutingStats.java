/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Captures weighted shard routing stats per node. See {@link WeightedRoutingService} for more details.
 *
 * @opensearch.internal
 */
public class WeightedRoutingStats implements ToXContentFragment, Writeable {
    // number of times fail open has to be executed for search requests
    private AtomicInteger failOpenCount;

    private static final WeightedRoutingStats INSTANCE = new WeightedRoutingStats();

    public static WeightedRoutingStats getInstance() {
        return INSTANCE;
    }

    private WeightedRoutingStats() {
        failOpenCount = new AtomicInteger(0);
    }

    public WeightedRoutingStats(StreamInput in) throws IOException {
        failOpenCount = new AtomicInteger(in.readInt());
    }

    public void updateFailOpenCount() {
        failOpenCount.getAndIncrement();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("weighted_routing");
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedRoutingStats that = (WeightedRoutingStats) o;
        return failOpenCount.equals(that.failOpenCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOpenCount);
    }

    public void resetFailOpenCount() {
        failOpenCount.set(0);
    }
}
