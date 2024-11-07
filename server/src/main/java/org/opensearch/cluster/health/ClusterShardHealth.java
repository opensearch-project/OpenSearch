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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

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

    /**
     * Constructor for ClusterShardHealth that takes detailed shard health metrics.
     *
     * @param shardId The unique identifier for this shard
     * @param status Current health status (GREEN/YELLOW/RED) of the shard:
     *               - GREEN: Primary and all replicas are active
     *               - YELLOW: Primary is active but some replicas are inactive
     *               - RED: Primary is inactive (except for scaled-down indices with active search replicas)
     * @param activeShards Number of shards currently active and able to handle requests
     * @param relocatingShards Number of shards currently being moved between nodes
     * @param initializingShards Number of shards currently initializing (e.g., recovering data)
     * @param unassignedShards Number of shards not currently assigned to any node
     * @param delayedUnassignedShards Number of unassigned shards whose allocation is delayed
     * @param isPrimaryActive Whether the primary shard is active. For scaled-down indices,
     *                       this will be false as primaries are removed while search replicas remain active
     */

    public ClusterShardHealth(
        int shardId,
        ClusterHealthStatus status,
        int activeShards,
        int relocatingShards,
        int initializingShards,
        int unassignedShards,
        int delayedUnassignedShards,
        boolean isPrimaryActive
    ) {
        this.shardId = shardId;
        this.status = status;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.primaryActive = isPrimaryActive;
    }

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
        // If primaryRouting is null or unassigned, check if it's due to remove_indexing_shards setting
        if (primaryRouting == null || primaryRouting.unassigned()) {
            // If we have any active shards (search replicas) due to remove_indexing_shards setting
            if (activeShards > 0) {
                return ClusterHealthStatus.GREEN;
            }
            return ClusterHealthStatus.RED;
        }

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

    /**
     * Analyzes shards to compute health statistics specifically for search-only shards.
     * This method examines each shard's state and aggregates metrics to determine
     * overall health status for search operations.
     *
     * @param shards List of shard routings to analyze
     * @return SearchShardStats containing aggregated metrics and health status:
     *         - hasSearchOnlyShards: true if any search-only shards exist
     *         - Counts of shards in different states (active/relocating/etc)
     *         - Overall status determined by:
     *           RED: No active shards available for search
     *           YELLOW: Has unassigned or initializing shards
     *           GREEN: All search shards healthy and active
     */
    static SearchShardStats computeSearchShardStatus(List<ShardRouting> shards) {
        int activeShards = 0;
        int relocatingShards = 0;
        int initializingShards = 0;
        int unassignedShards = 0;
        int delayedUnassignedShards = 0;
        boolean hasSearchOnlyShards = false;

        for (ShardRouting shard : shards) {
            if (shard != null && shard.isSearchOnly()) {
                hasSearchOnlyShards = true;
                if (shard.active()) {
                    activeShards++;
                    if (shard.relocating()) {
                        relocatingShards++;
                    }
                } else if (shard.initializing()) {
                    initializingShards++;
                } else if (shard.unassigned()) {
                    unassignedShards++;
                    if (Objects.requireNonNull(shard.unassignedInfo()).isDelayed()) {
                        delayedUnassignedShards++;
                    }
                }
            }
        }

        ClusterHealthStatus status;
        if (activeShards == 0) {
            status = ClusterHealthStatus.RED;
        } else if (unassignedShards > 0 || initializingShards > 0) {
            status = ClusterHealthStatus.YELLOW;
        } else {
            status = ClusterHealthStatus.GREEN;
        }

        return new SearchShardStats(
            hasSearchOnlyShards,
            activeShards,
            relocatingShards,
            initializingShards,
            unassignedShards,
            delayedUnassignedShards,
            status
        );
    }

    /**
     * Creates a shard health entry specifically for search-only shards in scaled-down indices.
     * This method creates and stores a ClusterShardHealth object that represents the health
     * status of a search-only shard (non-primary shard that only handles search operations).
     *
     * @param shardId The unique identifier for this shard
     * @param searchShardStats Contains aggregated health metrics for search-only shards including:
     *                        - Overall status (RED/YELLOW/GREEN)
     *                        - Counts of active/relocating/initializing/unassigned shards
     *                        - Information about delayed unassigned shards
     * @param computeShards Map to store the created shard health entry, keyed by shard ID
     */
    static void createSearchOnlyShardHealth(
        int shardId,
        SearchShardStats searchShardStats,
        Map<Integer, ClusterShardHealth> computeShards
    ) {
        computeShards.put(
            shardId,
            new ClusterShardHealth(
                shardId,
                searchShardStats.status,
                searchShardStats.activeShards,
                searchShardStats.relocatingShards,
                searchShardStats.initializingShards,
                searchShardStats.unassignedShards,
                searchShardStats.delayedUnassignedShards,
                false
            )
        );
    }

    /**
     * Computes health statistics for an individual shard and updates the cluster health stats.
     *
     * @param indexShardRoutingTable Routing table containing information about the shard replicas
     * @param shards Map to store the computed shard health status, keyed by shard ID
     * @param resultConsumer Consumer that gets called with the computed cluster health stats
     *                      to update aggregate metrics. Called with status (RED/YELLOW/GREEN),
     *                      counts of active/relocating/initializing/unassigned shards, and
     *                      whether primary is active.
     */
    static void computeShardLevelHealth(
        IndexShardRoutingTable indexShardRoutingTable,
        Map<Integer, ClusterShardHealth> shards,
        Consumer<ClusterIndexHealthStats> resultConsumer
    ) {
        int shardId = indexShardRoutingTable.shardId().id();
        ClusterShardHealth shardHealth = new ClusterShardHealth(shardId, indexShardRoutingTable);
        shards.put(shardId, shardHealth);

        resultConsumer.accept(
            new ClusterIndexHealthStats(
                shardHealth.getStatus(),
                shardHealth.isPrimaryActive() ? 1 : 0,
                shardHealth.getActiveShards(),
                shardHealth.getRelocatingShards(),
                shardHealth.getInitializingShards(),
                shardHealth.getUnassignedShards(),
                shardHealth.getDelayedUnassignedShards(),
                Collections.emptyMap()
            )
        );
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
