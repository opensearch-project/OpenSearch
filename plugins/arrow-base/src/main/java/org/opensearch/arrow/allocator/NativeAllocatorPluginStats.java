/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.opensearch.arrow.spi.NativeAllocatorPoolStats;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.PluginNodeStats;

import java.io.IOException;

/**
 * {@link PluginNodeStats} adapter that exposes {@link NativeAllocatorPoolStats}
 * as a top-level {@code native_allocator} entry under {@code _nodes/stats}.
 *
 * <p>Wraps the SPI-defined Arrow-agnostic snapshot so it can flow through the
 * generic plugin stats wire path without adding a server dependency to {@code arrow-spi}.
 */
public class NativeAllocatorPluginStats implements PluginNodeStats {

    /** NamedWriteable name and top-level field key under {@code nodes.<id>}. */
    public static final String NAME = "native_allocator";

    private final NativeAllocatorPoolStats poolStats;

    /**
     * Wraps the given snapshot.
     *
     * @param poolStats the pool stats snapshot to expose under {@code _nodes/stats}
     */
    public NativeAllocatorPluginStats(NativeAllocatorPoolStats poolStats) {
        this.poolStats = poolStats;
    }

    /**
     * Reads from the wire.
     *
     * @param in the stream input
     * @throws IOException if reading fails
     */
    public NativeAllocatorPluginStats(StreamInput in) throws IOException {
        this.poolStats = new NativeAllocatorPoolStats(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        poolStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // NativeAllocatorPoolStats wraps its body in {"native_allocator":{...}}; here we are
        // already inside a builder.startObject("native_allocator") opened by NodeStats.toXContent,
        // so emit only the inner fields.
        builder.startObject("root");
        builder.humanReadableField(
            "allocated_bytes",
            "allocated",
            new org.opensearch.core.common.unit.ByteSizeValue(poolStats.getRootAllocatedBytes())
        );
        builder.humanReadableField("peak_bytes", "peak", new org.opensearch.core.common.unit.ByteSizeValue(poolStats.getRootPeakBytes()));
        builder.humanReadableField(
            "limit_bytes",
            "limit",
            new org.opensearch.core.common.unit.ByteSizeValue(poolStats.getRootLimitBytes())
        );
        builder.endObject();

        builder.startObject("pools");
        for (NativeAllocatorPoolStats.PoolStats pool : poolStats.getPools()) {
            pool.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /** Returns the wrapped snapshot. */
    public NativeAllocatorPoolStats getPoolStats() {
        return poolStats;
    }
}
