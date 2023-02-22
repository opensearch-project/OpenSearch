/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.ShardIndexingPressureTracker;

import java.io.IOException;

/**
 * Per shard indexing pressure statistics
 *
 * @opensearch.internal
 */
public class IndexingPressurePerShardStats implements Writeable, ToXContentFragment {

    private final String shardId;

    private final long totalCombinedCoordinatingAndPrimaryBytes;
    private final long totalCoordinatingBytes;
    private final long totalPrimaryBytes;
    private final long totalReplicaBytes;

    private final long currentCombinedCoordinatingAndPrimaryBytes;
    private final long currentCoordinatingBytes;
    private final long currentPrimaryBytes;
    private final long currentReplicaBytes;

    private final long totalCoordinatingCount;
    private final long totalPrimaryCount;
    private final long totalReplicaCount;

    private final long coordinatingRejections;
    private final long coordinatingNodeLimitsBreachedRejections;
    private final long coordinatingLastSuccessfulRequestLimitsBreachedRejections;
    private final long coordinatingThroughputDegradationLimitsBreachedRejections;

    private final long primaryRejections;
    private final long primaryNodeLimitsBreachedRejections;
    private final long primaryLastSuccessfulRequestLimitsBreachedRejections;
    private final long primaryThroughputDegradationLimitsBreachedRejections;

    private final long replicaRejections;
    private final long replicaNodeLimitsBreachedRejections;
    private final long replicaLastSuccessfulRequestLimitsBreachedRejections;
    private final long replicaThroughputDegradationLimitsBreachedRejections;

    private final long coordinatingTimeInMillis;
    private final long primaryTimeInMillis;
    private final long replicaTimeInMillis;

    private final long coordinatingLastSuccessfulRequestTimestampInMillis;
    private final long primaryLastSuccessfulRequestTimestampInMillis;
    private final long replicaLastSuccessfulRequestTimestampInMillis;

    private final long currentPrimaryAndCoordinatingLimits;
    private final long currentReplicaLimits;

    private final boolean shardIndexingPressureEnforced;

    public IndexingPressurePerShardStats(StreamInput in) throws IOException {
        shardId = in.readString();
        shardIndexingPressureEnforced = in.readBoolean();

        totalCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        totalCoordinatingBytes = in.readVLong();
        totalPrimaryBytes = in.readVLong();
        totalReplicaBytes = in.readVLong();

        currentCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        currentCoordinatingBytes = in.readVLong();
        currentPrimaryBytes = in.readVLong();
        currentReplicaBytes = in.readVLong();

        totalCoordinatingCount = in.readVLong();
        totalPrimaryCount = in.readVLong();
        totalReplicaCount = in.readVLong();

        coordinatingRejections = in.readVLong();
        coordinatingNodeLimitsBreachedRejections = in.readVLong();
        coordinatingLastSuccessfulRequestLimitsBreachedRejections = in.readVLong();
        coordinatingThroughputDegradationLimitsBreachedRejections = in.readVLong();

        primaryRejections = in.readVLong();
        primaryNodeLimitsBreachedRejections = in.readVLong();
        primaryLastSuccessfulRequestLimitsBreachedRejections = in.readVLong();
        primaryThroughputDegradationLimitsBreachedRejections = in.readVLong();

        replicaRejections = in.readVLong();
        replicaNodeLimitsBreachedRejections = in.readVLong();
        replicaLastSuccessfulRequestLimitsBreachedRejections = in.readVLong();
        replicaThroughputDegradationLimitsBreachedRejections = in.readVLong();

        coordinatingTimeInMillis = in.readVLong();
        primaryTimeInMillis = in.readVLong();
        replicaTimeInMillis = in.readVLong();

        coordinatingLastSuccessfulRequestTimestampInMillis = in.readVLong();
        primaryLastSuccessfulRequestTimestampInMillis = in.readVLong();
        replicaLastSuccessfulRequestTimestampInMillis = in.readVLong();

        currentPrimaryAndCoordinatingLimits = in.readVLong();
        currentReplicaLimits = in.readVLong();
    }

    public IndexingPressurePerShardStats(ShardIndexingPressureTracker shardIndexingPressureTracker, boolean shardIndexingPressureEnforced) {

        shardId = shardIndexingPressureTracker.getShardId().toString();
        this.shardIndexingPressureEnforced = shardIndexingPressureEnforced;

        totalCombinedCoordinatingAndPrimaryBytes = shardIndexingPressureTracker.getCommonOperationTracker()
            .getTotalCombinedCoordinatingAndPrimaryBytes();
        totalCoordinatingBytes = shardIndexingPressureTracker.getCoordinatingOperationTracker().getStatsTracker().getTotalBytes();
        totalPrimaryBytes = shardIndexingPressureTracker.getPrimaryOperationTracker().getStatsTracker().getTotalBytes();
        totalReplicaBytes = shardIndexingPressureTracker.getReplicaOperationTracker().getStatsTracker().getTotalBytes();

        currentCombinedCoordinatingAndPrimaryBytes = shardIndexingPressureTracker.getCommonOperationTracker()
            .getCurrentCombinedCoordinatingAndPrimaryBytes();
        currentCoordinatingBytes = shardIndexingPressureTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes();
        currentPrimaryBytes = shardIndexingPressureTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes();
        currentReplicaBytes = shardIndexingPressureTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes();

        totalCoordinatingCount = shardIndexingPressureTracker.getCoordinatingOperationTracker().getStatsTracker().getRequestCount();
        totalPrimaryCount = shardIndexingPressureTracker.getPrimaryOperationTracker().getStatsTracker().getRequestCount();
        totalReplicaCount = shardIndexingPressureTracker.getReplicaOperationTracker().getStatsTracker().getRequestCount();

        coordinatingRejections = shardIndexingPressureTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections();
        coordinatingNodeLimitsBreachedRejections = shardIndexingPressureTracker.getCoordinatingOperationTracker()
            .getRejectionTracker()
            .getNodeLimitsBreachedRejections();
        coordinatingLastSuccessfulRequestLimitsBreachedRejections = shardIndexingPressureTracker.getCoordinatingOperationTracker()
            .getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections();
        coordinatingThroughputDegradationLimitsBreachedRejections = shardIndexingPressureTracker.getCoordinatingOperationTracker()
            .getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections();

        primaryRejections = shardIndexingPressureTracker.getPrimaryOperationTracker().getRejectionTracker().getTotalRejections();
        primaryNodeLimitsBreachedRejections = shardIndexingPressureTracker.getPrimaryOperationTracker()
            .getRejectionTracker()
            .getNodeLimitsBreachedRejections();
        primaryLastSuccessfulRequestLimitsBreachedRejections = shardIndexingPressureTracker.getPrimaryOperationTracker()
            .getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections();
        primaryThroughputDegradationLimitsBreachedRejections = shardIndexingPressureTracker.getPrimaryOperationTracker()
            .getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections();

        replicaRejections = shardIndexingPressureTracker.getReplicaOperationTracker().getRejectionTracker().getTotalRejections();
        replicaNodeLimitsBreachedRejections = shardIndexingPressureTracker.getReplicaOperationTracker()
            .getRejectionTracker()
            .getNodeLimitsBreachedRejections();
        replicaLastSuccessfulRequestLimitsBreachedRejections = shardIndexingPressureTracker.getReplicaOperationTracker()
            .getRejectionTracker()
            .getLastSuccessfulRequestLimitsBreachedRejections();
        replicaThroughputDegradationLimitsBreachedRejections = shardIndexingPressureTracker.getReplicaOperationTracker()
            .getRejectionTracker()
            .getThroughputDegradationLimitsBreachedRejections();

        coordinatingTimeInMillis = shardIndexingPressureTracker.getCoordinatingOperationTracker()
            .getPerformanceTracker()
            .getLatencyInMillis();
        primaryTimeInMillis = shardIndexingPressureTracker.getPrimaryOperationTracker().getPerformanceTracker().getLatencyInMillis();
        replicaTimeInMillis = shardIndexingPressureTracker.getReplicaOperationTracker().getPerformanceTracker().getLatencyInMillis();

        coordinatingLastSuccessfulRequestTimestampInMillis = shardIndexingPressureTracker.getCoordinatingOperationTracker()
            .getPerformanceTracker()
            .getLastSuccessfulRequestTimestamp();
        primaryLastSuccessfulRequestTimestampInMillis = shardIndexingPressureTracker.getPrimaryOperationTracker()
            .getPerformanceTracker()
            .getLastSuccessfulRequestTimestamp();
        replicaLastSuccessfulRequestTimestampInMillis = shardIndexingPressureTracker.getReplicaOperationTracker()
            .getPerformanceTracker()
            .getLastSuccessfulRequestTimestamp();

        currentPrimaryAndCoordinatingLimits = shardIndexingPressureTracker.getPrimaryAndCoordinatingLimits();
        currentReplicaLimits = shardIndexingPressureTracker.getReplicaLimits();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(shardId);
        out.writeBoolean(shardIndexingPressureEnforced);

        out.writeVLong(totalCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(totalCoordinatingBytes);
        out.writeVLong(totalPrimaryBytes);
        out.writeVLong(totalReplicaBytes);

        out.writeVLong(currentCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(currentCoordinatingBytes);
        out.writeVLong(currentPrimaryBytes);
        out.writeVLong(currentReplicaBytes);

        out.writeVLong(totalCoordinatingCount);
        out.writeVLong(totalPrimaryCount);
        out.writeVLong(totalReplicaCount);

        out.writeVLong(coordinatingRejections);
        out.writeVLong(coordinatingNodeLimitsBreachedRejections);
        out.writeVLong(coordinatingLastSuccessfulRequestLimitsBreachedRejections);
        out.writeVLong(coordinatingThroughputDegradationLimitsBreachedRejections);

        out.writeVLong(primaryRejections);
        out.writeVLong(primaryNodeLimitsBreachedRejections);
        out.writeVLong(primaryLastSuccessfulRequestLimitsBreachedRejections);
        out.writeVLong(primaryThroughputDegradationLimitsBreachedRejections);

        out.writeVLong(replicaRejections);
        out.writeVLong(replicaNodeLimitsBreachedRejections);
        out.writeVLong(replicaLastSuccessfulRequestLimitsBreachedRejections);
        out.writeVLong(replicaThroughputDegradationLimitsBreachedRejections);

        out.writeVLong(coordinatingTimeInMillis);
        out.writeVLong(primaryTimeInMillis);
        out.writeVLong(replicaTimeInMillis);

        out.writeVLong(coordinatingLastSuccessfulRequestTimestampInMillis);
        out.writeVLong(primaryLastSuccessfulRequestTimestampInMillis);
        out.writeVLong(replicaLastSuccessfulRequestTimestampInMillis);

        out.writeVLong(currentPrimaryAndCoordinatingLimits);
        out.writeVLong(currentReplicaLimits);
    }

    public long getTotalCombinedCoordinatingAndPrimaryBytes() {
        return totalCombinedCoordinatingAndPrimaryBytes;
    }

    public long getTotalCoordinatingBytes() {
        return totalCoordinatingBytes;
    }

    public long getTotalPrimaryBytes() {
        return totalPrimaryBytes;
    }

    public long getTotalReplicaBytes() {
        return totalReplicaBytes;
    }

    public long getCurrentCombinedCoordinatingAndPrimaryBytes() {
        return currentCombinedCoordinatingAndPrimaryBytes;
    }

    public long getCurrentCoordinatingBytes() {
        return currentCoordinatingBytes;
    }

    public long getCurrentPrimaryBytes() {
        return currentPrimaryBytes;
    }

    public long getCurrentReplicaBytes() {
        return currentReplicaBytes;
    }

    public long getCoordinatingRejections() {
        return coordinatingRejections;
    }

    public long getCoordinatingNodeLimitsBreachedRejections() {
        return coordinatingNodeLimitsBreachedRejections;
    }

    public long getCoordinatingLastSuccessfulRequestLimitsBreachedRejections() {
        return coordinatingLastSuccessfulRequestLimitsBreachedRejections;
    }

    public long getCoordinatingThroughputDegradationLimitsBreachedRejections() {
        return coordinatingThroughputDegradationLimitsBreachedRejections;
    }

    public long getPrimaryRejections() {
        return primaryRejections;
    }

    public long getPrimaryNodeLimitsBreachedRejections() {
        return primaryNodeLimitsBreachedRejections;
    }

    public long getPrimaryLastSuccessfulRequestLimitsBreachedRejections() {
        return primaryLastSuccessfulRequestLimitsBreachedRejections;
    }

    public long getPrimaryThroughputDegradationLimitsBreachedRejections() {
        return primaryThroughputDegradationLimitsBreachedRejections;
    }

    public long getReplicaRejections() {
        return replicaRejections;
    }

    public long getReplicaNodeLimitsBreachedRejections() {
        return replicaNodeLimitsBreachedRejections;
    }

    public long getReplicaLastSuccessfulRequestLimitsBreachedRejections() {
        return replicaLastSuccessfulRequestLimitsBreachedRejections;
    }

    public long getReplicaThroughputDegradationLimitsBreachedRejections() {
        return replicaThroughputDegradationLimitsBreachedRejections;
    }

    public long getCurrentPrimaryAndCoordinatingLimits() {
        return currentPrimaryAndCoordinatingLimits;
    }

    public long getCurrentReplicaLimits() {
        return currentReplicaLimits;
    }

    private static final String COORDINATING = "coordinating";
    private static final String COORDINATING_IN_BYTES = "coordinating_in_bytes";
    private static final String COORDINATING_COUNT = "coordinating_count";
    private static final String PRIMARY = "primary";
    private static final String PRIMARY_IN_BYTES = "primary_in_bytes";
    private static final String PRIMARY_COUNT = "primary_count";
    private static final String REPLICA = "replica";
    private static final String REPLICA_IN_BYTES = "replica_in_bytes";
    private static final String REPLICA_COUNT = "replica_count";
    private static final String COORDINATING_REJECTIONS = "coordinating_rejections";
    private static final String PRIMARY_REJECTIONS = "primary_rejections";
    private static final String REPLICA_REJECTIONS = "replica_rejections";
    private static final String BREAKUP_NODE_LIMITS = "node_limits";
    private static final String BREAKUP_NO_SUCCESSFUL_REQUEST_LIMITS = "no_successful_request_limits";
    private static final String BREAKUP_THROUGHPUT_DEGRADATION_LIMIT = "throughput_degradation_limits";
    private static final String COORDINATING_TIME_IN_MILLIS = "coordinating_time_in_millis";
    private static final String PRIMARY_TIME_IN_MILLIS = "primary_time_in_millis";
    private static final String REPLICA_TIME_IN_MILLIS = "replica_time_in_millis";
    private static final String COORDINATING_LAST_SUCCESSFUL_REQUEST_TIMESTAMP_IN_MILLIS =
        "coordinating_last_successful_request_timestamp_in_millis";
    private static final String PRIMARY_LAST_SUCCESSFUL_REQUEST_TIMESTAMP_IN_MILLIS = "primary_last_successful_request_timestamp_in_millis";
    private static final String REPLICA_LAST_SUCCESSFUL_REQUEST_TIMESTAMP_IN_MILLIS = "replica_last_successful_request_timestamp_in_millis";
    private static final String CURRENT_COORDINATING_AND_PRIMARY_LIMITS_IN_BYTES = "current_coordinating_and_primary_limits_in_bytes";
    private static final String CURRENT_REPLICA_LIMITS_IN_BYTES = "current_replica_limits_in_bytes";
    private static final String CURRENT_COORDINATING_AND_PRIMARY_IN_BYTES = "current_coordinating_and_primary_bytes";
    private static final String CURRENT_REPLICA_IN_BYTES = "current_replica_bytes";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(shardId);

        builder.startObject("memory");
        builder.startObject("current");
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(currentCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(currentPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(currentReplicaBytes));
        builder.endObject();
        builder.startObject("total");
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(totalCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(totalPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(totalReplicaBytes));
        builder.endObject();
        builder.endObject();

        builder.startObject("rejection");
        builder.startObject("coordinating");
        builder.field(COORDINATING_REJECTIONS, coordinatingRejections);
        if (shardIndexingPressureEnforced) {
            builder.startObject("breakup");
        } else {
            builder.startObject("breakup_shadow_mode");
        }
        builder.field(BREAKUP_NODE_LIMITS, coordinatingNodeLimitsBreachedRejections);
        builder.field(BREAKUP_NO_SUCCESSFUL_REQUEST_LIMITS, coordinatingLastSuccessfulRequestLimitsBreachedRejections);
        builder.field(BREAKUP_THROUGHPUT_DEGRADATION_LIMIT, coordinatingThroughputDegradationLimitsBreachedRejections);
        builder.endObject();
        builder.endObject();
        builder.startObject("primary");
        builder.field(PRIMARY_REJECTIONS, primaryRejections);
        if (shardIndexingPressureEnforced) {
            builder.startObject("breakup");
        } else {
            builder.startObject("breakup_shadow_mode");
        }
        builder.field(BREAKUP_NODE_LIMITS, primaryNodeLimitsBreachedRejections);
        builder.field(BREAKUP_NO_SUCCESSFUL_REQUEST_LIMITS, primaryLastSuccessfulRequestLimitsBreachedRejections);
        builder.field(BREAKUP_THROUGHPUT_DEGRADATION_LIMIT, primaryThroughputDegradationLimitsBreachedRejections);
        builder.endObject();
        builder.endObject();
        builder.startObject("replica");
        builder.field(REPLICA_REJECTIONS, replicaRejections);
        if (shardIndexingPressureEnforced) {
            builder.startObject("breakup");
        } else {
            builder.startObject("breakup_shadow_mode");
        }
        builder.field(BREAKUP_NODE_LIMITS, replicaNodeLimitsBreachedRejections);
        builder.field(BREAKUP_NO_SUCCESSFUL_REQUEST_LIMITS, replicaLastSuccessfulRequestLimitsBreachedRejections);
        builder.field(BREAKUP_THROUGHPUT_DEGRADATION_LIMIT, replicaThroughputDegradationLimitsBreachedRejections);
        builder.endObject();
        builder.endObject();
        builder.endObject();

        builder.startObject("last_successful_timestamp");
        builder.field(COORDINATING_LAST_SUCCESSFUL_REQUEST_TIMESTAMP_IN_MILLIS, coordinatingLastSuccessfulRequestTimestampInMillis);
        builder.field(PRIMARY_LAST_SUCCESSFUL_REQUEST_TIMESTAMP_IN_MILLIS, primaryLastSuccessfulRequestTimestampInMillis);
        builder.field(REPLICA_LAST_SUCCESSFUL_REQUEST_TIMESTAMP_IN_MILLIS, replicaLastSuccessfulRequestTimestampInMillis);
        builder.endObject();

        builder.startObject("indexing");
        builder.field(COORDINATING_TIME_IN_MILLIS, coordinatingTimeInMillis);
        builder.field(COORDINATING_COUNT, totalCoordinatingCount);
        builder.field(PRIMARY_TIME_IN_MILLIS, primaryTimeInMillis);
        builder.field(PRIMARY_COUNT, totalPrimaryCount);
        builder.field(REPLICA_TIME_IN_MILLIS, replicaTimeInMillis);
        builder.field(REPLICA_COUNT, totalReplicaCount);
        builder.endObject();

        builder.startObject("memory_allocation");
        builder.startObject("current");
        builder.field(CURRENT_COORDINATING_AND_PRIMARY_IN_BYTES, currentCombinedCoordinatingAndPrimaryBytes);
        builder.field(CURRENT_REPLICA_IN_BYTES, currentReplicaBytes);
        builder.endObject();
        builder.startObject("limit");
        builder.field(CURRENT_COORDINATING_AND_PRIMARY_LIMITS_IN_BYTES, currentPrimaryAndCoordinatingLimits);
        builder.field(CURRENT_REPLICA_LIMITS_IN_BYTES, currentReplicaLimits);
        builder.endObject();
        builder.endObject();

        return builder.endObject();
    }
}
