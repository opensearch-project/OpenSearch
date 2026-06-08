/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Snapshot of node-local analytics-engine counters and timers. Built by
 * {@link AnalyticsStatsCollector#snapshot()} and rendered as a JSON fragment
 * by the stats REST handler.
 *
 * <p>Field naming aligns with PR #21660's {@code StageProfile} ({@code elapsed_ms},
 * {@code rows_processed}, {@code tasks_completed}) so dashboards can ingest
 * either API without translating field names.
 *
 * <p>Latency buckets carry only raw cumulative {@code count} and {@code sum_ms}
 * — mirroring the analytics DataFusion backend's stats contract. Averages,
 * percentiles, and rates are derived by the metrics backend (Tumbler /
 * Prometheus / etc.) from interval diffs.
 *
 * <p>Marked {@link ExperimentalApi} — field shapes and the bucket layout may
 * change in subsequent revisions.
 */
@ExperimentalApi
public final class AnalyticsStats implements ToXContentFragment, Writeable {

    private final Queries queries;
    private final Map<String, StageBucket> stagesByType;
    private final Fragments fragments;

    public AnalyticsStats(Queries queries, Map<String, StageBucket> stagesByType, Fragments fragments) {
        this.queries = queries;
        this.stagesByType = stagesByType;
        this.fragments = fragments;
    }

    public AnalyticsStats(StreamInput in) throws IOException {
        this.queries = new Queries(in);
        int size = in.readVInt();
        // TreeMap preserves the deterministic alphabetical ordering used by snapshot().
        Map<String, StageBucket> map = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            map.put(key, new StageBucket(in));
        }
        this.stagesByType = map;
        this.fragments = new Fragments(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        queries.writeTo(out);
        out.writeVInt(stagesByType.size());
        for (Map.Entry<String, StageBucket> e : stagesByType.entrySet()) {
            out.writeString(e.getKey());
            e.getValue().writeTo(out);
        }
        fragments.writeTo(out);
    }

    public Queries queries() {
        return queries;
    }

    public Map<String, StageBucket> stagesByType() {
        return stagesByType;
    }

    public Fragments fragments() {
        return fragments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("analytics");
        queries.toXContent(builder, params);
        builder.startObject("stages_by_type");
        for (Map.Entry<String, StageBucket> e : stagesByType.entrySet()) {
            builder.field(e.getKey());
            e.getValue().toXContent(builder, params);
        }
        builder.endObject();
        fragments.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Cumulative latency totals: {@code count} of recordings and {@code sum_ms}
     * of their elapsed times. Both monotonically increasing since node start.
     */
    public record LatencyStats(long count, long sumMs) implements ToXContentFragment, Writeable {

        public static final LatencyStats EMPTY = new LatencyStats(0, 0);

        public LatencyStats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(count);
            out.writeVLong(sumMs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", count);
            builder.field("sum_ms", sumMs);
            builder.endObject();
            return builder;
        }
    }

    /**
     * Per-query latency rollup. Counters like total / succeeded / failed are
     * intentionally <strong>not</strong> exposed here — those are covered by
     * analytics-engine integration with the existing {@code _nodes/stats}
     * extension point. This bucket carries analytics-engine-specific timing
     * totals that don't have a home in core node stats: end-to-end query
     * elapsed and Calcite planning time.
     */
    public record Queries(LatencyStats elapsedMs, LatencyStats planningMs) implements ToXContentFragment, Writeable {

        public Queries(StreamInput in) throws IOException {
            this(new LatencyStats(in), new LatencyStats(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            elapsedMs.writeTo(out);
            planningMs.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("queries");
            builder.field("elapsed_ms");
            elapsedMs.toXContent(builder, params);
            builder.field("planning_ms");
            planningMs.toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }

    /** Per-stage-type rollup, keyed by {@link org.opensearch.analytics.planner.dag.StageExecutionType} name. */
    public record StageBucket(long started, long succeeded, long failed, long cancelled, long rowsProcessedTotal, LatencyStats elapsedMs)
        implements
            ToXContentFragment,
            Writeable {

        public StageBucket(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), new LatencyStats(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(started);
            out.writeVLong(succeeded);
            out.writeVLong(failed);
            out.writeVLong(cancelled);
            out.writeVLong(rowsProcessedTotal);
            elapsedMs.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("started", started);
            builder.field("succeeded", succeeded);
            builder.field("failed", failed);
            builder.field("cancelled", cancelled);
            builder.field("rows_processed_total", rowsProcessedTotal);
            builder.field("elapsed_ms");
            elapsedMs.toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }

    /** Per-fragment (task) rollup. */
    public record Fragments(long total, long succeeded, long failed, LatencyStats elapsedMs) implements ToXContentFragment, Writeable {

        public Fragments(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), new LatencyStats(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(total);
            out.writeVLong(succeeded);
            out.writeVLong(failed);
            elapsedMs.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("fragments");
            builder.field("total", total);
            builder.field("succeeded", succeeded);
            builder.field("failed", failed);
            builder.field("elapsed_ms");
            elapsedMs.toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }
}
