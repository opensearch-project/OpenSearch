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

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Cluster Index Health Information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ClusterIndexHealth implements Iterable<ClusterShardHealth>, Writeable, ToXContentFragment {
    private static final String STATUS = "status";
    private static final String NUMBER_OF_SHARDS = "number_of_shards";
    private static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    private static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String SHARDS = "shards";

    private static final ConstructingObjectParser<ClusterIndexHealth, String> PARSER = new ConstructingObjectParser<>(
        "cluster_index_health",
        true,
        (parsedObjects, index) -> {
            int i = 0;
            int numberOfShards = (int) parsedObjects[i++];
            int numberOfReplicas = (int) parsedObjects[i++];
            int activeShards = (int) parsedObjects[i++];
            int relocatingShards = (int) parsedObjects[i++];
            int initializingShards = (int) parsedObjects[i++];
            int unassignedShards = (int) parsedObjects[i++];
            int activePrimaryShards = (int) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i++];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            @SuppressWarnings("unchecked")
            List<ClusterShardHealth> shardList = (List<ClusterShardHealth>) parsedObjects[i];
            final Map<Integer, ClusterShardHealth> shards;
            if (shardList == null || shardList.isEmpty()) {
                shards = emptyMap();
            } else {
                shards = new HashMap<>(shardList.size());
                for (ClusterShardHealth shardHealth : shardList) {
                    shards.put(shardHealth.getShardId(), shardHealth);
                }
            }
            return new ClusterIndexHealth(
                index,
                numberOfShards,
                numberOfReplicas,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                activePrimaryShards,
                status,
                shards
            );
        }
    );

    public static final ObjectParser.NamedObjectParser<ClusterShardHealth, String> SHARD_PARSER = (
        XContentParser p,
        String indexIgnored,
        String shardId) -> ClusterShardHealth.innerFromXContent(p, Integer.valueOf(shardId));

    static {
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_REPLICAS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_PRIMARY_SHARDS));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
        // Can be absent if LEVEL == 'indices' or 'cluster'
        PARSER.declareNamedObjects(optionalConstructorArg(), SHARD_PARSER, new ParseField(SHARDS));
    }

    private final String index;
    private final int numberOfShards;
    private final int numberOfReplicas;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private int delayedUnassignedShards;
    private final int activePrimaryShards;
    private final ClusterHealthStatus status;
    private final Map<Integer, ClusterShardHealth> shards;

    /**
     * Constructors for creating health metrics for a cluster index.
     * The simple constructor defaults to SHARDS level health checks:
     * @param indexMetadata Metadata for the index being checked
     * @param indexRoutingTable Routing table for the index shards
     */

    public ClusterIndexHealth(final IndexMetadata indexMetadata, final IndexRoutingTable indexRoutingTable) {
        this(indexMetadata, indexRoutingTable, ClusterHealthRequest.Level.SHARDS);
    }

    /**
     * The full constructor allows specifying health check detail level and handles scaled-down indices:
     * @param indexMetadata Metadata for the index being checked
     * @param indexRoutingTable Routing table for the index shards
     * @param healthLevel Detail level for health checks (cluster/indices/shards)
     */
    public ClusterIndexHealth(
        final IndexMetadata indexMetadata,
        final IndexRoutingTable indexRoutingTable,
        final ClusterHealthRequest.Level healthLevel
    ) {
        this.index = indexMetadata.getIndex().getName();
        this.numberOfShards = indexMetadata.getNumberOfShards();
        this.numberOfReplicas = indexMetadata.getNumberOfReplicas();

        boolean removeIndexingShards = indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_REMOVE_INDEXING_SHARDS, false);

        ClusterIndexHealthStats result = removeIndexingShards
            ? computeScaledDownIndicesHealth(indexRoutingTable)
            : computeRegularIndicesHealth(indexRoutingTable, healthLevel);

        this.status = result.status;
        this.activePrimaryShards = result.activePrimaryShards;
        this.activeShards = result.activeShards;
        this.relocatingShards = result.relocatingShards;
        this.initializingShards = result.initializingShards;
        this.unassignedShards = result.unassignedShards;
        this.delayedUnassignedShards = result.delayedUnassignedShards;
        this.shards = result.shards;
    }

    /**
     * Computes health metrics for scaled-down indices, which are indices where SETTING_REMOVE_INDEXING_SHARDS is true.
     * This method specifically focuses on search-only shards and excludes indexing shards from health calculations.
     *
     * @param indexRoutingTable The routing table containing shard allocation information for the scaled-down index
     * @return ClusterIndexHealthStats containing aggregated health metrics for the scaled-down index
     * <p>
     * Key differences from regular index health computation:
     * - Only considers search-only shards in health calculations
     * - Always reports zero active primary shards since scaled-down indices don't maintain primaries
     * - Creates shard health entries only for shards that have search-only copies
     * <p>
     * Health metrics include:
     * - Active shards (search-only replicas)
     * - Relocating shards
     * - Initializing shards
     * - Unassigned shards
     * - Delayed unassigned shards
     * - Overall status (GREEN/YELLOW/RED) based only on search-only shards
     * <p>
     * The method aggregates metrics across all shards but maintains the special semantics
     * of scaled-down indices where indexing operations are not supported.
     */

    private ClusterIndexHealthStats computeScaledDownIndicesHealth(IndexRoutingTable indexRoutingTable) {
        final IndexHealthStats indexHealthStats = new IndexHealthStats();
        final Map<Integer, ClusterShardHealth> computeShards = new HashMap<>();

        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            if (indexShardRoutingTable == null) {
                continue;
            }

            List<ShardRouting> shards = indexShardRoutingTable.shards();
            int shardId = indexShardRoutingTable.shardId().id();
            SearchShardStats searchShardStats = ClusterShardHealth.computeSearchShardStatus(shards);

            indexHealthStats.activeShards += searchShardStats.activeShards;
            indexHealthStats.relocatingShards += searchShardStats.relocatingShards;
            indexHealthStats.initializingShards += searchShardStats.initializingShards;
            indexHealthStats.unassignedShards += searchShardStats.unassignedShards;
            indexHealthStats.delayedUnassignedShards += searchShardStats.delayedUnassignedShards;

            if (searchShardStats.hasSearchOnlyShards) {
                ClusterShardHealth.createSearchOnlyShardHealth(shardId, searchShardStats, computeShards);
            }

            indexHealthStats.status = getIndexHealthStatus(searchShardStats.status, indexHealthStats.status);
        }

        return new ClusterIndexHealthStats(
            indexHealthStats.status,
            0,
            indexHealthStats.activeShards,
            indexHealthStats.relocatingShards,
            indexHealthStats.initializingShards,
            indexHealthStats.unassignedShards,
            indexHealthStats.delayedUnassignedShards,
            computeShards
        );
    }

    /**
     * Computes health metrics for regular (non-scaled-down) indices. This method processes each shard's routing table
     * and aggregates health statistics at either the shard or index level based on the requested health level.
     *
     * @param indexRoutingTable The routing table containing shard allocation information for the index
     * @param healthLevel The requested detail level for health checks (cluster/indices/shards)
     * @return ClusterIndexHealthStats containing aggregated health metrics for the index
     * <p>
     * The method handles two levels of health computation:
     * 1. Shard-level (Level.SHARDS): Detailed health metrics for each individual shard
     * 2. Index-level: Aggregated health metrics without per-shard details
     * <p>
     * Health metrics include:
     * - Active primary and replica shards
     * - Relocating shards
     * - Initializing shards
     * - Unassigned shards
     * - Overall status (GREEN/YELLOW/RED)
     * <p>
     * If any shard's routing table is null, the index is marked as RED status.
     * For empty routing tables (no shards), the index is also marked as RED.
     */

    private ClusterIndexHealthStats computeRegularIndicesHealth(
        IndexRoutingTable indexRoutingTable,
        ClusterHealthRequest.Level healthLevel
    ) {
        final IndexHealthStats indexHealthStats = new IndexHealthStats();
        final Map<Integer, ClusterShardHealth> computeShards = new HashMap<>();
        final boolean isShardLevelHealthRequired = healthLevel == ClusterHealthRequest.Level.SHARDS;

        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            if (indexShardRoutingTable == null) {
                indexHealthStats.status = ClusterHealthStatus.RED;
                continue;
            }

            if (isShardLevelHealthRequired) {
                ClusterShardHealth.computeShardLevelHealth(indexShardRoutingTable, computeShards, result -> {
                    indexHealthStats.activePrimaryShards += result.activePrimaryShards;
                    indexHealthStats.activeShards += result.activeShards;
                    indexHealthStats.relocatingShards += result.relocatingShards;
                    indexHealthStats.initializingShards += result.initializingShards;
                    indexHealthStats.unassignedShards += result.unassignedShards;
                    indexHealthStats.delayedUnassignedShards += result.delayedUnassignedShards;
                    indexHealthStats.status = getIndexHealthStatus(result.status, indexHealthStats.status);
                });
            } else {
                computeIndexLevelHealth(indexShardRoutingTable, result -> {
                    indexHealthStats.activePrimaryShards += result.activePrimaryShards;
                    indexHealthStats.activeShards += result.activeShards;
                    indexHealthStats.relocatingShards += result.relocatingShards;
                    indexHealthStats.initializingShards += result.initializingShards;
                    indexHealthStats.unassignedShards += result.unassignedShards;
                    indexHealthStats.delayedUnassignedShards += result.delayedUnassignedShards;
                    indexHealthStats.status = getIndexHealthStatus(result.status, indexHealthStats.status);
                });
            }
        }

        if (indexRoutingTable.shards() != null && indexRoutingTable.shards().isEmpty()) {
            indexHealthStats.status = ClusterHealthStatus.RED;
        }

        return new ClusterIndexHealthStats(
            indexHealthStats.status,
            indexHealthStats.activePrimaryShards,
            indexHealthStats.activeShards,
            indexHealthStats.relocatingShards,
            indexHealthStats.initializingShards,
            indexHealthStats.unassignedShards,
            indexHealthStats.delayedUnassignedShards,
            computeShards
        );
    }

    /**
     * Computes health metrics at the index level without generating per-shard details.
     * This method processes a single shard's routing table to calculate aggregate health statistics
     * and determines the overall health status based on the primary shard's state.
     *
     * @param indexShardRoutingTable The routing table for a specific shard in the index
     * @param resultConsumer A consumer that receives the computed health statistics
     * <p>
     * Health metrics computed include:
     * - Active shards count
     * - Relocating shards count (subset of active shards that are currently relocating)
     * - Initializing shards count
     * - Unassigned shards count
     * - Delayed unassigned shards count (subset of unassigned shards that are deliberately delayed)
     * <p>
     * The overall status is determined as follows:
     * - RED if the primary shard is null or inactive
     * - Otherwise, status is based on the primary shard's health calculation
     * <p>
     * Note: This method is used when shard-level health details are not required
     * (i.e., when health check level is 'indices' or 'cluster').
     */
    private void computeIndexLevelHealth(IndexShardRoutingTable indexShardRoutingTable, Consumer<ClusterIndexHealthStats> resultConsumer) {
        int activeShards = 0;
        int relocatingShards = 0;
        int initializingShards = 0;
        int unassignedShards = 0;
        int delayedUnassignedShards = 0;

        List<ShardRouting> shardRoutings = indexShardRoutingTable.shards();
        for (ShardRouting shardRouting : shardRoutings) {
            if (shardRouting.active()) {
                activeShards++;
                if (shardRouting.relocating()) {
                    relocatingShards++;
                }
            } else if (shardRouting.initializing()) {
                initializingShards++;
            } else if (shardRouting.unassigned()) {
                unassignedShards++;
                if (Objects.requireNonNull(shardRouting.unassignedInfo()).isDelayed()) {
                    delayedUnassignedShards++;
                }
            }
        }

        ShardRouting primaryShard = indexShardRoutingTable.primaryShard();
        ClusterHealthStatus status = primaryShard != null
            ? ClusterShardHealth.getShardHealth(primaryShard, activeShards, shardRoutings.size())
            : ClusterHealthStatus.RED;

        resultConsumer.accept(
            new ClusterIndexHealthStats(
                status,
                primaryShard != null && primaryShard.active() ? 1 : 0,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                delayedUnassignedShards,
                Collections.emptyMap()
            )
        );
    }

    public static ClusterHealthStatus getIndexHealthStatus(ClusterHealthStatus shardHealth, ClusterHealthStatus computeStatus) {
        switch (shardHealth) {
            case RED:
                return ClusterHealthStatus.RED;
            case YELLOW:
                // do not override an existing red
                if (computeStatus != ClusterHealthStatus.RED) {
                    return ClusterHealthStatus.YELLOW;
                } else {
                    return ClusterHealthStatus.RED;
                }
            default:
                return computeStatus;
        }
    }

    public ClusterIndexHealth(final StreamInput in) throws IOException {
        index = in.readString();
        numberOfShards = in.readVInt();
        numberOfReplicas = in.readVInt();
        activePrimaryShards = in.readVInt();
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());

        int size = in.readVInt();
        shards = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            ClusterShardHealth shardHealth = new ClusterShardHealth(in);
            shards.put(shardHealth.getShardId(), shardHealth);
        }
    }

    /**
     * For XContent Parser and serialization tests
     */
    ClusterIndexHealth(
        String index,
        int numberOfShards,
        int numberOfReplicas,
        int activeShards,
        int relocatingShards,
        int initializingShards,
        int unassignedShards,
        int activePrimaryShards,
        ClusterHealthStatus status,
        Map<Integer, ClusterShardHealth> shards
    ) {
        this.index = index;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.activePrimaryShards = activePrimaryShards;
        this.status = status;
        this.shards = shards;
    }

    public String getIndex() {
        return index;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
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

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<Integer, ClusterShardHealth> getShards() {
        return this.shards;
    }

    @Override
    public Iterator<ClusterShardHealth> iterator() {
        return shards.values().iterator();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(numberOfShards);
        out.writeVInt(numberOfReplicas);
        out.writeVInt(activePrimaryShards);
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeByte(status.value());
        out.writeCollection(shards.values());
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(getIndex());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(NUMBER_OF_SHARDS, getNumberOfShards());
        builder.field(NUMBER_OF_REPLICAS, getNumberOfReplicas());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());

        if ("shards".equals(params.param("level", "indices"))) {
            builder.startObject(SHARDS);
            // Only include shard details if we have actual shards to report
            if (!shards.isEmpty()) {
                for (ClusterShardHealth shardHealth : shards.values()) {
                    shardHealth.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static ClusterIndexHealth innerFromXContent(XContentParser parser, String index) {
        return PARSER.apply(parser, index);
    }

    public static ClusterIndexHealth fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String index = parser.currentName();
        ClusterIndexHealth parsed = innerFromXContent(parser, index);
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return parsed;
    }

    @Override
    public String toString() {
        return "ClusterIndexHealth{"
            + "index='"
            + index
            + '\''
            + ", numberOfShards="
            + numberOfShards
            + ", numberOfReplicas="
            + numberOfReplicas
            + ", activeShards="
            + activeShards
            + ", relocatingShards="
            + relocatingShards
            + ", initializingShards="
            + initializingShards
            + ", unassignedShards="
            + unassignedShards
            + ", activePrimaryShards="
            + activePrimaryShards
            + ", status="
            + status
            + ", shards.size="
            + (shards == null ? "null" : shards.size())
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterIndexHealth that = (ClusterIndexHealth) o;
        return Objects.equals(index, that.index)
            && numberOfShards == that.numberOfShards
            && numberOfReplicas == that.numberOfReplicas
            && activeShards == that.activeShards
            && relocatingShards == that.relocatingShards
            && initializingShards == that.initializingShards
            && unassignedShards == that.unassignedShards
            && activePrimaryShards == that.activePrimaryShards
            && status == that.status
            && Objects.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            index,
            numberOfShards,
            numberOfReplicas,
            activeShards,
            relocatingShards,
            initializingShards,
            unassignedShards,
            activePrimaryShards,
            status,
            shards
        );
    }
}
