/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Migration status object.
 */
public class TieringStatus implements ToXContentObject, Writeable {

    private static final Logger logger = LogManager.getLogger(TieringStatus.class);

    /** Key for tiering status. */
    public static final String TIERING_STATUS = "tiering_status";
    /** Key for index name. */
    public static final String INDEX = "index";
    /** Key for state. */
    public static final String STATE = "state";
    /** Key for source tier. */
    public static final String SOURCE = "source";
    /** Key for target tier. */
    public static final String TARGET = "target";
    /** Key for start time. */
    public static final String START_TIME = "start_time";
    /** Key for shard level status. */
    public static final String SHARD_LEVEL_STATUS = "shard_level_status";
    /** Key for succeeded shards count. */
    public static final String SUCCEEDED_SHARDS = "succeeded";
    /** Key for running shards count. */
    public static final String RUNNING_SHARDS = "running";
    /** Key for pending shards count. */
    public static final String PENDING_SHARDS = "pending";
    /** Key for total shards count. */
    public static final String TOTAL_SHARDS = "total";
    /** Key for shard relocation status. */
    public static final String SHARD_RELOCATION_STATUS = "shard_relocation_status";
    /** Key for relocating node ID. */
    public static final String RELOCATING_NODE_ID = "relocating_node_id";
    /** Key for source shard ID. */
    public static final String SOURCE_SHARD_ID = "source_shard_id";

    private ShardLevelStatus shardLevelStatus = null;
    private String indexName;
    private String status;
    private String source;
    private String target;
    private long startTime;

    /** Returns the index name. */
    public String getIndexName() {
        return indexName;
    }

    /** Returns the shard level status. */
    public ShardLevelStatus getShardLevelStatus() {
        return shardLevelStatus;
    }

    /**
     * Sets the shard level status.
     * @param shardLevelStatus the shard level status
     */
    public void setShardLevelStatus(ShardLevelStatus shardLevelStatus) {
        this.shardLevelStatus = shardLevelStatus;
    }

    /**
     * Sets the index name.
     * @param indexName the index name
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /** Returns the status. */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the status.
     * @param status the status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /** Returns the start time. */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Sets the start time.
     * @param startTime the start time
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /** Returns the source tier. */
    public String getSource() {
        return this.source;
    }

    /**
     * Sets the source tier.
     * @param source the source tier
     */
    public void setSource(String source) {
        this.source = source;
    }

    /** Returns the target tier. */
    public String getTarget() {
        return target;
    }

    /**
     * Sets the target tier.
     * @param target the target tier
     */
    public void setTarget(String target) {
        this.target = target;
    }

    /**
     * Constructs a TieringStatus with the given parameters.
     * @param indexName the index name
     * @param status the status
     * @param source the source tier
     * @param target the target tier
     * @param startTime the start time
     */
    public TieringStatus(final String indexName, final String status, final String source, final String target, final long startTime) {
        this.indexName = indexName;
        this.status = status;
        this.source = source;
        this.target = target;
        this.startTime = startTime;
    }

    /** Constructs an empty TieringStatus. */
    public TieringStatus() {}

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeString(indexName);
        streamOutput.writeString(status);
        streamOutput.writeString(source);
        streamOutput.writeString(target);
        streamOutput.writeLong(startTime);
        streamOutput.writeOptionalWriteable(shardLevelStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(TIERING_STATUS);
        xContentBuilder.field(INDEX, this.getIndexName());
        xContentBuilder.field(STATE, this.getStatus());
        xContentBuilder.field(SOURCE, this.getSource());
        xContentBuilder.field(TARGET, this.getTarget());
        xContentBuilder.field(START_TIME, this.getStartTime());
        if (shardLevelStatus != null) {
            shardLevelStatus.toXContent(xContentBuilder, params);
        }
        xContentBuilder.endObject();
        return xContentBuilder;
    }

    /**
     * Reads a TieringStatus from stream.
     * @param in the stream input
     * @return the tiering status
     * @throws IOException if error
     */
    public static TieringStatus readFrom(StreamInput in) throws IOException {
        final TieringStatus tieringStatus = new TieringStatus();
        tieringStatus.setIndexName(in.readString());
        tieringStatus.setStatus(in.readString());
        tieringStatus.setSource(in.readString());
        tieringStatus.setTarget(in.readString());
        tieringStatus.setStartTime(in.readLong());
        tieringStatus.setShardLevelStatus(in.readOptionalWriteable(ShardLevelStatus::new));

        return tieringStatus;
    }

    /** Shard-level status with counters. */
    public static class ShardLevelStatus implements ToXContentObject, Writeable {
        private final Map<String, Integer> shardLevelCounters;

        /** Returns ongoing shards. */
        public List<OngoingShard> getOngoingShards() {
            return ongoingShards;
        }

        private final List<OngoingShard> ongoingShards;

        /** Returns shard level counters. */
        public Map<String, Integer> getShardLevelCounters() {
            return shardLevelCounters;
        }

        /**
         * Constructs from stream.
         * @param in the stream input
         * @throws IOException if error
         */
        public ShardLevelStatus(StreamInput in) throws IOException {
            shardLevelCounters = new HashMap<>();
            final Map<String, Object> inputMap = in.readMap();
            for (Map.Entry<String, Object> entry : inputMap.entrySet()) {
                shardLevelCounters.put(entry.getKey(), Integer.parseInt(entry.getValue().toString()));
            }
            List<OngoingShard> shards = in.readList(OngoingShard::new);
            ongoingShards = shards != null ? shards : Collections.emptyList();
        }

        /**
         * Constructs with counters and shards.
         * @param shardLevelCounters the counters
         * @param ongoingShards the ongoing shards
         */
        public ShardLevelStatus(Map<String, Integer> shardLevelCounters, List<OngoingShard> ongoingShards) {
            this.shardLevelCounters = shardLevelCounters;
            this.ongoingShards = ongoingShards;
        }

        public static ShardLevelStatus fromRoutingTable(
            ClusterState clusterState,
            String index,
            Boolean isDetailedFlagEnabled,
            String targetTier
        ) {
            final List<ShardRouting> routingTable = clusterState.routingTable().allShards(index);
            final Map<String, Integer> shardLevelCounters = new HashMap<>();
            final List<OngoingShard> ongoingShards = new ArrayList<>();

            int pending = 0;
            int running = 0;
            int succeeded = 0;
            boolean isTargetWarm = "WARM".equalsIgnoreCase(targetTier);

            for (ShardRouting shard : routingTable) {
                // Switch based on shard routing state. Note that INITIALIZING is missing below since we will have a
                // corresponding shard in RELOCATING state.
                switch (shard.state()) {
                    case STARTED:
                        // Only count STARTED shards as done if they're placed on target nodes.
                        final boolean isWarmNode = clusterState.getNodes().get(shard.currentNodeId()).isWarmNode();
                        if (isTargetWarm == isWarmNode) {
                            succeeded++;
                        } else {
                            pending++;
                        }
                        break;
                    case UNASSIGNED:
                        pending++;
                        break;
                    case RELOCATING:
                        running++;
                        if (isDetailedFlagEnabled) {
                            ongoingShards.add(new OngoingShard(shard.shardId().id(), shard.relocatingNodeId()));
                        }
                        break;
                    default:
                        logger.warn("Unexpected shard state [{}] for index [{}]", shard.state(), index);
                        break;

                }
            }
            shardLevelCounters.put(PENDING_SHARDS, pending);
            shardLevelCounters.put(SUCCEEDED_SHARDS, succeeded);
            shardLevelCounters.put(RUNNING_SHARDS, running);
            shardLevelCounters.put(TOTAL_SHARDS, pending + succeeded + running);
            return new ShardLevelStatus(shardLevelCounters, ongoingShards);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMapWithConsistentOrder(shardLevelCounters);
            out.writeList(ongoingShards != null ? ongoingShards : Collections.emptyList());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(SHARD_LEVEL_STATUS);
            for (Map.Entry<String, Integer> counter : shardLevelCounters.entrySet()) {
                builder.field(counter.getKey(), counter.getValue());
            }

            if (ongoingShards != null && !ongoingShards.isEmpty()) {
                builder.startArray(SHARD_RELOCATION_STATUS);
                for (OngoingShard shard : ongoingShards) {
                    if (shard != null) {
                        shard.toXContent(builder, params);
                    }
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShardLevelStatus that = (ShardLevelStatus) o;
            return Objects.equals(shardLevelCounters, that.shardLevelCounters) && Objects.equals(ongoingShards, that.ongoingShards);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardLevelCounters, ongoingShards);
        }
    }

    /** Represents an ongoing shard relocation. */
    public static class OngoingShard implements ToXContentObject, Writeable {
        private final int sourceShardId;
        private final String relocatingNodeId;

        /**
         * Constructs an OngoingShard.
         * @param shardId the shard ID
         * @param relocatingNodeId the relocating node ID
         */
        public OngoingShard(int shardId, final String relocatingNodeId) {
            this.sourceShardId = shardId;
            this.relocatingNodeId = relocatingNodeId;
        }

        /**
         * Constructs from stream.
         * @param in the stream input
         * @throws IOException if error
         */
        public OngoingShard(StreamInput in) throws IOException {
            this.sourceShardId = in.readInt();
            this.relocatingNodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(sourceShardId);
            out.writeString(relocatingNodeId != null ? relocatingNodeId : "");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field(SOURCE_SHARD_ID, sourceShardId).field(RELOCATING_NODE_ID, relocatingNodeId).endObject();
            return builder;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TieringStatus that = (TieringStatus) o;
        return Objects.equals(indexName, that.indexName)
            && Objects.equals(status, that.status)
            && Objects.equals(source, that.source)
            && Objects.equals(target, that.target)
            && (startTime == that.startTime)
            && Objects.equals(shardLevelStatus, that.shardLevelStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, status, source, target, startTime, shardLevelStatus);
    }
}
