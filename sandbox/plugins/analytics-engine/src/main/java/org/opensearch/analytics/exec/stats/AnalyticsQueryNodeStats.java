/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.PluginNodeStats;

import java.io.IOException;

/**
 * Node-level analytics query stats exposed via {@code GET /_nodes/stats/plugin_stats}.
 * Appears under {@code nodes.<id>.analytics_query_stats} in the response.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryNodeStats implements PluginNodeStats {

    public static final String NAME = "analytics_query_stats";

    private final long totalQueries;
    private final long failedQueries;
    private final long cancelledQueries;
    private final long planningP50Ms;
    private final long planningP95Ms;
    private final long planningP99Ms;
    private final long executionP50Ms;
    private final long executionP95Ms;
    private final long executionP99Ms;
    private final long coordinatorRowsP50;
    private final long coordinatorRowsP95;
    private final long shardFragmentStageCount;
    private final long shardFragmentStageTotalMs;
    private final long coordinatorReduceStageCount;
    private final long coordinatorReduceStageTotalMs;

    public AnalyticsQueryNodeStats(
        long totalQueries,
        long failedQueries,
        long cancelledQueries,
        long planningP50Ms,
        long planningP95Ms,
        long planningP99Ms,
        long executionP50Ms,
        long executionP95Ms,
        long executionP99Ms,
        long coordinatorRowsP50,
        long coordinatorRowsP95,
        long shardFragmentStageCount,
        long shardFragmentStageTotalMs,
        long coordinatorReduceStageCount,
        long coordinatorReduceStageTotalMs
    ) {
        this.totalQueries = totalQueries;
        this.failedQueries = failedQueries;
        this.cancelledQueries = cancelledQueries;
        this.planningP50Ms = planningP50Ms;
        this.planningP95Ms = planningP95Ms;
        this.planningP99Ms = planningP99Ms;
        this.executionP50Ms = executionP50Ms;
        this.executionP95Ms = executionP95Ms;
        this.executionP99Ms = executionP99Ms;
        this.coordinatorRowsP50 = coordinatorRowsP50;
        this.coordinatorRowsP95 = coordinatorRowsP95;
        this.shardFragmentStageCount = shardFragmentStageCount;
        this.shardFragmentStageTotalMs = shardFragmentStageTotalMs;
        this.coordinatorReduceStageCount = coordinatorReduceStageCount;
        this.coordinatorReduceStageTotalMs = coordinatorReduceStageTotalMs;
    }

    public AnalyticsQueryNodeStats(StreamInput in) throws IOException {
        this.totalQueries = in.readVLong();
        this.failedQueries = in.readVLong();
        this.cancelledQueries = in.readVLong();
        this.planningP50Ms = in.readVLong();
        this.planningP95Ms = in.readVLong();
        this.planningP99Ms = in.readVLong();
        this.executionP50Ms = in.readVLong();
        this.executionP95Ms = in.readVLong();
        this.executionP99Ms = in.readVLong();
        this.coordinatorRowsP50 = in.readVLong();
        this.coordinatorRowsP95 = in.readVLong();
        this.shardFragmentStageCount = in.readVLong();
        this.shardFragmentStageTotalMs = in.readVLong();
        this.coordinatorReduceStageCount = in.readVLong();
        this.coordinatorReduceStageTotalMs = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalQueries);
        out.writeVLong(failedQueries);
        out.writeVLong(cancelledQueries);
        out.writeVLong(planningP50Ms);
        out.writeVLong(planningP95Ms);
        out.writeVLong(planningP99Ms);
        out.writeVLong(executionP50Ms);
        out.writeVLong(executionP95Ms);
        out.writeVLong(executionP99Ms);
        out.writeVLong(coordinatorRowsP50);
        out.writeVLong(coordinatorRowsP95);
        out.writeVLong(shardFragmentStageCount);
        out.writeVLong(shardFragmentStageTotalMs);
        out.writeVLong(coordinatorReduceStageCount);
        out.writeVLong(coordinatorReduceStageTotalMs);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("query_counts");
        builder.field("total", totalQueries);
        builder.field("failed", failedQueries);
        builder.field("cancelled", cancelledQueries);
        builder.endObject();

        builder.startObject("planning_time_ms");
        builder.field("p50", planningP50Ms);
        builder.field("p95", planningP95Ms);
        builder.field("p99", planningP99Ms);
        builder.endObject();

        builder.startObject("execution_time_ms");
        builder.field("p50", executionP50Ms);
        builder.field("p95", executionP95Ms);
        builder.field("p99", executionP99Ms);
        builder.endObject();

        builder.startObject("coordinator_rows");
        builder.field("p50", coordinatorRowsP50);
        builder.field("p95", coordinatorRowsP95);
        builder.endObject();

        builder.startObject("stages");
        builder.startObject("shard_fragment");
        builder.field("count", shardFragmentStageCount);
        builder.field("total_ms", shardFragmentStageTotalMs);
        builder.endObject();
        builder.startObject("coordinator_reduce");
        builder.field("count", coordinatorReduceStageCount);
        builder.field("total_ms", coordinatorReduceStageTotalMs);
        builder.endObject();
        builder.endObject();

        return builder;
    }
}
