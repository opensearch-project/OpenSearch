/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;

/**
 * Live, node-level snapshot of the {@code parquet_native_write} thread pool that backs Parquet
 * background ingestion writes. Surfaces queue saturation and rejection pressure.
 *
 * <p>Node-level only: rendered under the {@code native_ingest_pool} block of the per-node stats
 * aggregate, never on a per-shard payload.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetIngestPoolStats implements Writeable, ToXContentFragment {

    private final int threads;
    private final int queueDepth;
    private final int active;
    private final long rejected;
    private final long completed;

    public ParquetIngestPoolStats(int threads, int queueDepth, int active, long rejected, long completed) {
        this.threads = threads;
        this.queueDepth = queueDepth;
        this.active = active;
        this.rejected = rejected;
        this.completed = completed;
    }

    public ParquetIngestPoolStats(StreamInput in) throws IOException {
        this.threads = in.readVInt();
        this.queueDepth = in.readVInt();
        this.active = in.readVInt();
        this.rejected = in.readVLong();
        this.completed = in.readVLong();
    }

    /**
     * Builds a snapshot from a {@link ThreadPoolStats.Stats} reading of the {@code parquet_native_write}
     * pool, or {@code null} if {@code stats} is {@code null} (pool not found).
     */
    public static ParquetIngestPoolStats from(ThreadPoolStats.Stats stats) {
        if (stats == null) {
            return null;
        }
        return new ParquetIngestPoolStats(
            stats.getThreads(),
            stats.getQueue(),
            stats.getActive(),
            stats.getRejected(),
            stats.getCompleted()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(threads);
        out.writeVInt(queueDepth);
        out.writeVInt(active);
        out.writeVLong(rejected);
        out.writeVLong(completed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("native_ingest_pool");
        builder.field("threads", threads);
        builder.field("queue_depth", queueDepth);
        builder.field("active", active);
        builder.field("rejected", rejected);
        builder.field("completed", completed);
        builder.endObject();
        return builder;
    }
}
