/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.health;

import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Cluster shard health information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ClusterShardHealth implements Writeable, ToXContentFragment {
    private static final String STATUS = "status";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String PRIMARY_ACTIVE = "primary_active";

    public static final ConstructingObjectParser<ClusterShardHealth, Integer> PARSER = new ConstructingObjectParser<>(
        "cluster_shard_health",
        true,
        (parsedObjects, shardId) -> {
            int i = 0;
            boolean primaryActive = (boolean) parsedObjects[i++];
            int activeShards = (int) parsedObjects[i++];
            int relocatingShards = (int) parsedObjects[i++];
            int initializingShards = (int) parsedObjects[i++];
            int unassignedShards = (int) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            return new ClusterShardHealth(
                shardId,
                status,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                primaryActive
            );
        }
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(PRIMARY_ACTIVE));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
    }

    private final int shardId;
    private final ClusterHealthStatus status;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private int delayedUnassignedShards;
    private final boolean primaryActive;

    public ClusterShardHealth(final int shardId, final IndexShardRoutingTable shardRoutingTable) {
        this.shardId = shardId;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        int computeDelayedUnassignedShards = 0;
        List<ShardRouting> shardRoutings = shardRoutingTable.shards();
        for (int index = 0; index < shardRoutings.size(); index++) {
            ShardRouting shardRouting = shardRoutings.get(index);
            if (shardRouting.active()) {
                computeActiveShards++;
                if (shardRouting.relocating()) {
                    // the shard is relocating, the one it is relocating to will be in initializing state, so we don't count it
                    computeRelocatingShards++;
                }
            } else if (shardRouting.initializing()) {
                computeInitializingShards++;
            } else if (shardRouting.unassigned()) {
                computeUnassignedShards++;
                if (shardRouting.unassignedInfo() != null && shardRouting.unassignedInfo().isDelayed()) {
                    computeDelayedUnassignedShards++;
                }
            }
        }
        final ShardRouting primaryRouting = shardRoutingTable.primaryShard();
        this.status = getShardHealth(primaryRouting, computeActiveShards, shardRoutingTable.size());
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;
        this.delayedUnassignedShards = computeDelayedUnassignedShards;
        this.primaryActive = primaryRouting.active();
    }

    public ClusterShardHealth(final StreamInput in) throws IOException {
        shardId = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        primaryActive = in.readBoolean();
    }

    /**
     * For XContent Parser and serialization tests
     */
    ClusterShardHealth(
        int shardId,
        ClusterHealthStatus status,
        int activeShards,
        int relocatingShards,
        int initializingShards,
        int unassignedShards,
        boolean primaryActive
    ) {
        this.shardId = shardId;
        this.status = status;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.primaryActive = primaryActive;
    }

    public int getShardId() {
        return shardId;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public boolean isPrimaryActive() {
        return primaryActive;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public int getDelayedUnassignedShards() {
        return delayedUnassignedShards;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeByte(status.value());
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeBoolean(primaryActive);
    }

    /**
     * Computes the shard health of an index.
     * <p>
     *     Shard health is GREEN when all primary and replica shards of the indices are active.
     *     Shard health is YELLOW when primary shard is active but at-least one replica shard is inactive.
     *     Shard health is RED when the primary is not active.
     * </p>
     */
    public static ClusterHealthStatus getShardHealth(final ShardRouting primaryRouting, final int activeShards, final int totalShards) {
        assert primaryRouting != null : "Primary shard routing can't be null";
        if (primaryRouting.active()) {
            if (activeShards == totalShards) {
                return ClusterHealthStatus.GREEN;
            } else {
                return ClusterHealthStatus.YELLOW;
            }
        } else {
            return getInactivePrimaryHealth(primaryRouting);
        }
    }

    /**
     * Checks if an inactive primary shard should cause the cluster health to go RED.
     * <p>
     * An inactive primary shard in an index should cause the cluster health to be RED to make it visible that some of the existing data is
     * unavailable. In case of index creation, snapshot restore or index shrinking, which are unexceptional events in the cluster lifecycle,
     * cluster health should not turn RED for the time where primaries are still in the initializing state but go to YELLOW instead.
     * However, in case of exceptional events, for example when the primary shard cannot be assigned to a node or initialization fails at
     * some point, cluster health should still turn RED.
     * <p>
     * NB: this method should *not* be called on active shards nor on non-primary shards.
     */
    public static ClusterHealthStatus getInactivePrimaryHealth(final ShardRouting shardRouting) {
        assert shardRouting.primary() : "cannot invoke on a replica shard: " + shardRouting;
        assert shardRouting.active() == false : "cannot invoke on an active shard: " + shardRouting;
        assert shardRouting.unassignedInfo() != null : "cannot invoke on a shard with no UnassignedInfo: " + shardRouting;
        assert shardRouting.recoverySource() != null : "cannot invoke on a shard that has no recovery source" + shardRouting;
        final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        RecoverySource.Type recoveryType = shardRouting.recoverySource().getType();
        if (unassignedInfo.getLastAllocationStatus() != AllocationStatus.DECIDERS_NO
            && unassignedInfo.getNumFailedAllocations() == 0
            && (recoveryType == RecoverySource.Type.EMPTY_STORE
                || recoveryType == RecoverySource.Type.LOCAL_SHARDS
                || recoveryType == RecoverySource.Type.SNAPSHOT)) {
            return ClusterHealthStatus.YELLOW;
        } else {
            return ClusterHealthStatus.RED;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Integer.toString(getShardId()));
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(PRIMARY_ACTIVE, isPrimaryActive());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.endObject();
        return builder;
    }

    static ClusterShardHealth innerFromXContent(XContentParser parser, Integer shardId) {
        return PARSER.apply(parser, shardId);
    }

    public static ClusterShardHealth fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String shardIdStr = parser.currentName();
        ClusterShardHealth parsed = innerFromXContent(parser, Integer.valueOf(shardIdStr));
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return parsed;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterShardHealth)) return false;
        ClusterShardHealth that = (ClusterShardHealth) o;
        return shardId == that.shardId
            && activeShards == that.activeShards
            && relocatingShards == that.relocatingShards
            && initializingShards == that.initializingShards
            && unassignedShards == that.unassignedShards
            && primaryActive == that.primaryActive
            && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, status, activeShards, relocatingShards, initializingShards, unassignedShards, primaryActive);
    }
}
