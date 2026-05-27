/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Shard-level statistics collector for the Composite data format plugin.
 * Tracks aggregate counters across all formats and per-format breakdown stats.
 * Uses LongAdder for high-throughput counters and AtomicLong for gauges.
 */
@ExperimentalApi
public class CompositeShardStats implements ToXContentFragment, Writeable {

    // Indexing counters (aggregate)
    private final LongAdder docsIndexedTotal = new LongAdder();
    private final LongAdder indexTimeMillis = new LongAdder();

    // Refresh counters (aggregate)
    private final LongAdder refreshTotal = new LongAdder();
    private final LongAdder refreshTimeMillis = new LongAdder();

    // Merge counters (aggregate)
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTimeMillis = new LongAdder();
    private final LongAdder mergeSegmentsInputTotal = new LongAdder();

    // Flush counters (aggregate)
    private final LongAdder flushTotal = new LongAdder();
    private final LongAdder flushTimeMillis = new LongAdder();

    // Sync counters (aggregate)
    private final LongAdder syncTotal = new LongAdder();
    private final LongAdder syncTimeMillis = new LongAdder();

    // Memory gauge
    private final AtomicLong nativeBytesUsed = new AtomicLong();

    // Per-format breakdown
    private final ConcurrentHashMap<String, FormatStats> perFormatStats = new ConcurrentHashMap<>();

    public CompositeShardStats() {}

    public CompositeShardStats(StreamInput in) throws IOException {
        // Indexing
        docsIndexedTotal.add(in.readVLong());
        indexTimeMillis.add(in.readVLong());

        // Refresh
        refreshTotal.add(in.readVLong());
        refreshTimeMillis.add(in.readVLong());

        // Merge
        mergeTotal.add(in.readVLong());
        mergeTimeMillis.add(in.readVLong());
        mergeSegmentsInputTotal.add(in.readVLong());

        // Flush
        flushTotal.add(in.readVLong());
        flushTimeMillis.add(in.readVLong());

        // Sync
        syncTotal.add(in.readVLong());
        syncTimeMillis.add(in.readVLong());

        // Memory
        nativeBytesUsed.set(in.readVLong());

        // Per-format
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String formatName = in.readString();
            perFormatStats.put(formatName, new FormatStats(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Indexing
        out.writeVLong(docsIndexedTotal.sum());
        out.writeVLong(indexTimeMillis.sum());

        // Refresh
        out.writeVLong(refreshTotal.sum());
        out.writeVLong(refreshTimeMillis.sum());

        // Merge
        out.writeVLong(mergeTotal.sum());
        out.writeVLong(mergeTimeMillis.sum());
        out.writeVLong(mergeSegmentsInputTotal.sum());

        // Flush
        out.writeVLong(flushTotal.sum());
        out.writeVLong(flushTimeMillis.sum());

        // Sync
        out.writeVLong(syncTotal.sum());
        out.writeVLong(syncTimeMillis.sum());

        // Memory
        out.writeVLong(nativeBytesUsed.get());

        // Per-format
        out.writeVInt(perFormatStats.size());
        for (Map.Entry<String, FormatStats> entry : perFormatStats.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Indexing
        builder.startObject("indexing");
        builder.field("docs_indexed_total", docsIndexedTotal.sum());
        builder.field("index_time_millis", indexTimeMillis.sum());
        builder.endObject();

        // Refresh
        builder.startObject("refresh");
        builder.field("refresh_total", refreshTotal.sum());
        builder.field("refresh_time_millis", refreshTimeMillis.sum());
        builder.endObject();

        // Merge
        builder.startObject("merge");
        builder.field("merges_total", mergeTotal.sum());
        builder.field("merge_time_millis", mergeTimeMillis.sum());
        builder.field("merge_segments_input_total", mergeSegmentsInputTotal.sum());
        builder.endObject();

        // Flush
        builder.startObject("flush");
        builder.field("flush_total", flushTotal.sum());
        builder.field("flush_time_millis", flushTimeMillis.sum());
        builder.endObject();

        // Sync
        builder.startObject("sync");
        builder.field("sync_total", syncTotal.sum());
        builder.field("sync_time_millis", syncTimeMillis.sum());
        builder.endObject();

        // Memory
        builder.startObject("memory");
        builder.field("native_bytes_used", nativeBytesUsed.get());
        builder.endObject();

        // Per-format breakdown
        builder.startObject("per_format");
        for (Map.Entry<String, FormatStats> entry : perFormatStats.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    /**
     * Returns an immutable snapshot of the current counters. All fields are read once,
     * so the resulting object reflects a consistent point-in-time view (no torn reads).
     */
    public CompositeShardStats snapshot() {
        CompositeShardStats snap = new CompositeShardStats();
        snap.docsIndexedTotal.add(this.docsIndexedTotal.sum());
        snap.indexTimeMillis.add(this.indexTimeMillis.sum());
        snap.refreshTotal.add(this.refreshTotal.sum());
        snap.refreshTimeMillis.add(this.refreshTimeMillis.sum());
        snap.mergeTotal.add(this.mergeTotal.sum());
        snap.mergeTimeMillis.add(this.mergeTimeMillis.sum());
        snap.mergeSegmentsInputTotal.add(this.mergeSegmentsInputTotal.sum());
        snap.flushTotal.add(this.flushTotal.sum());
        snap.flushTimeMillis.add(this.flushTimeMillis.sum());
        snap.syncTotal.add(this.syncTotal.sum());
        snap.syncTimeMillis.add(this.syncTimeMillis.sum());
        snap.nativeBytesUsed.set(this.nativeBytesUsed.get());
        for (Map.Entry<String, FormatStats> e : this.perFormatStats.entrySet()) {
            snap.perFormatStats.put(e.getKey(), e.getValue().snapshot());
        }
        return snap;
    }

    /**
     * Creates an aggregated stats instance by summing counters and taking max for gauges
     * across multiple shard stats.
     */
    public static CompositeShardStats aggregate(Collection<CompositeShardStats> shardStats) {
        CompositeShardStats agg = new CompositeShardStats();
        for (CompositeShardStats s : shardStats) {
            agg.docsIndexedTotal.add(s.docsIndexedTotal.sum());
            agg.indexTimeMillis.add(s.indexTimeMillis.sum());
            agg.refreshTotal.add(s.refreshTotal.sum());
            agg.refreshTimeMillis.add(s.refreshTimeMillis.sum());
            agg.mergeTotal.add(s.mergeTotal.sum());
            agg.mergeTimeMillis.add(s.mergeTimeMillis.sum());
            agg.mergeSegmentsInputTotal.add(s.mergeSegmentsInputTotal.sum());
            agg.flushTotal.add(s.flushTotal.sum());
            agg.flushTimeMillis.add(s.flushTimeMillis.sum());
            agg.syncTotal.add(s.syncTotal.sum());
            agg.syncTimeMillis.add(s.syncTimeMillis.sum());
            agg.nativeBytesUsed.addAndGet(s.nativeBytesUsed.get());
            for (Map.Entry<String, FormatStats> entry : s.perFormatStats.entrySet()) {
                agg.getOrCreateFormatStats(entry.getKey()).addFrom(entry.getValue());
            }
        }
        return agg;
    }

    /**
     * Adds all counters from another CompositeShardStats instance into this one.
     * Used by OldShardsStats accumulation when shards are closed/relocated.
     */
    public void addFrom(CompositeShardStats other) {
        docsIndexedTotal.add(other.docsIndexedTotal.sum());
        indexTimeMillis.add(other.indexTimeMillis.sum());
        refreshTotal.add(other.refreshTotal.sum());
        refreshTimeMillis.add(other.refreshTimeMillis.sum());
        mergeTotal.add(other.mergeTotal.sum());
        mergeTimeMillis.add(other.mergeTimeMillis.sum());
        mergeSegmentsInputTotal.add(other.mergeSegmentsInputTotal.sum());
        flushTotal.add(other.flushTotal.sum());
        flushTimeMillis.add(other.flushTimeMillis.sum());
        syncTotal.add(other.syncTotal.sum());
        syncTimeMillis.add(other.syncTimeMillis.sum());
        nativeBytesUsed.addAndGet(other.nativeBytesUsed.get());
        for (Map.Entry<String, FormatStats> entry : other.perFormatStats.entrySet()) {
            getOrCreateFormatStats(entry.getKey()).addFrom(entry.getValue());
        }
    }

    /**
     * Lazily creates and returns per-format stats for the given format name.
     */
    public FormatStats getOrCreateFormatStats(String formatName) {
        return perFormatStats.computeIfAbsent(formatName, k -> new FormatStats());
    }

    // --- Indexing methods ---

    public void addDocsIndexed(long n) {
        docsIndexedTotal.add(n);
    }

    public void addIndexTimeMillis(long ms) {
        indexTimeMillis.add(ms);
    }

    // --- Refresh methods ---

    public void incRefreshTotal() {
        refreshTotal.increment();
    }

    public void addRefreshTimeMillis(long ms) {
        refreshTimeMillis.add(ms);
    }

    // --- Merge methods ---

    public void incMergeTotal() {
        mergeTotal.increment();
    }

    public void addMergeTimeMillis(long ms) {
        mergeTimeMillis.add(ms);
    }

    public void addMergeSegmentsInputTotal(long n) {
        mergeSegmentsInputTotal.add(n);
    }

    // --- Flush methods ---

    public void incFlushTotal() {
        flushTotal.increment();
    }

    public void addFlushTimeMillis(long ms) {
        flushTimeMillis.add(ms);
    }

    // --- Sync methods ---

    public void incSyncTotal() {
        syncTotal.increment();
    }

    public void addSyncTimeMillis(long ms) {
        syncTimeMillis.add(ms);
    }

    // --- Memory gauge methods ---

    public void setNativeBytesUsed(long bytes) {
        nativeBytesUsed.set(bytes);
    }

    /**
     * Per-format statistics breakdown. Tracks indexing, refresh, merge, flush,
     * and sort metrics for an individual data format within the composite engine.
     */
    @ExperimentalApi
    public static class FormatStats implements ToXContentFragment, Writeable {

        private final LongAdder docsIndexedTotal = new LongAdder();
        private final LongAdder indexTimeMillis = new LongAdder();
        private final LongAdder indexFailures = new LongAdder();
        private final LongAdder refreshTimeMillis = new LongAdder();
        private final LongAdder mergeTimeMillis = new LongAdder();
        private final LongAdder mergeFailures = new LongAdder();
        private final LongAdder flushTimeMillis = new LongAdder();
        private final LongAdder sortTimeMillis = new LongAdder();

        public FormatStats() {}

        public FormatStats(StreamInput in) throws IOException {
            docsIndexedTotal.add(in.readVLong());
            indexTimeMillis.add(in.readVLong());
            indexFailures.add(in.readVLong());
            refreshTimeMillis.add(in.readVLong());
            mergeTimeMillis.add(in.readVLong());
            mergeFailures.add(in.readVLong());
            flushTimeMillis.add(in.readVLong());
            sortTimeMillis.add(in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(docsIndexedTotal.sum());
            out.writeVLong(indexTimeMillis.sum());
            out.writeVLong(indexFailures.sum());
            out.writeVLong(refreshTimeMillis.sum());
            out.writeVLong(mergeTimeMillis.sum());
            out.writeVLong(mergeFailures.sum());
            out.writeVLong(flushTimeMillis.sum());
            out.writeVLong(sortTimeMillis.sum());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("indexing");
            builder.field("docs_indexed_total", docsIndexedTotal.sum());
            builder.field("index_failures", indexFailures.sum());
            builder.field("index_time_millis", indexTimeMillis.sum());
            builder.endObject();

            builder.startObject("flush");
            builder.field("flush_time_millis", flushTimeMillis.sum());
            builder.endObject();

            builder.startObject("refresh");
            builder.field("refresh_time_millis", refreshTimeMillis.sum());
            builder.endObject();

            builder.startObject("merge");
            builder.field("merge_time_millis", mergeTimeMillis.sum());
            builder.field("merge_failures", mergeFailures.sum());
            builder.endObject();

            builder.startObject("sort");
            builder.field("sort_time_millis", sortTimeMillis.sum());
            builder.endObject();

            return builder;
        }

        // --- FormatStats mutators ---

        public void addDocsIndexed(long n) {
            docsIndexedTotal.add(n);
        }

        public void addIndexTimeMillis(long ms) {
            indexTimeMillis.add(ms);
        }

        public void incIndexFailures() {
            indexFailures.increment();
        }

        public void addRefreshTimeMillis(long ms) {
            refreshTimeMillis.add(ms);
        }

        public void addMergeTimeMillis(long ms) {
            mergeTimeMillis.add(ms);
        }

        public void incMergeFailures() {
            mergeFailures.increment();
        }

        public void addFlushTimeMillis(long ms) {
            flushTimeMillis.add(ms);
        }

        public void addSortTimeMillis(long ms) {
            sortTimeMillis.add(ms);
        }

        /**
         * Adds all counters from another FormatStats instance into this one.
         */
        public void addFrom(FormatStats other) {
            docsIndexedTotal.add(other.docsIndexedTotal.sum());
            indexTimeMillis.add(other.indexTimeMillis.sum());
            indexFailures.add(other.indexFailures.sum());
            refreshTimeMillis.add(other.refreshTimeMillis.sum());
            mergeTimeMillis.add(other.mergeTimeMillis.sum());
            mergeFailures.add(other.mergeFailures.sum());
            flushTimeMillis.add(other.flushTimeMillis.sum());
            sortTimeMillis.add(other.sortTimeMillis.sum());
        }

        /**
         * Returns an immutable snapshot of the current counters for this format.
         */
        public FormatStats snapshot() {
            FormatStats snap = new FormatStats();
            snap.docsIndexedTotal.add(this.docsIndexedTotal.sum());
            snap.indexTimeMillis.add(this.indexTimeMillis.sum());
            snap.indexFailures.add(this.indexFailures.sum());
            snap.refreshTimeMillis.add(this.refreshTimeMillis.sum());
            snap.mergeTimeMillis.add(this.mergeTimeMillis.sum());
            snap.mergeFailures.add(this.mergeFailures.sum());
            snap.flushTimeMillis.add(this.flushTimeMillis.sum());
            snap.sortTimeMillis.add(this.sortTimeMillis.sum());
            return snap;
        }
    }
}
