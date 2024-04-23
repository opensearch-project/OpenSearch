/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.cluster.metadata.IndexMetadata.REMOTE_STORE_CUSTOM_KEY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING;

/**
 * Utils for checking and mutating cluster state during remote migration
 *
 * @opensearch.internal
 */
public class RemoteMigrationIndexMetadataUpdater {
    private final Logger logger;

    public RemoteMigrationIndexMetadataUpdater(Logger logger) {
        this.logger = logger;
    }

    /**
     * During docrep to remote store migration, applies the following remote store based index settings
     * once all shards of an index have moved over to remote store enabled nodes
     * <br>
     * Also appends the requisite Remote Store Path based custom metadata to the existing index metadata
     */
    public void maybeAddRemoteIndexSettings(
        IndexMetadata indexMetadata,
        IndexMetadata.Builder indexMetadataBuilder,
        RoutingTable routingTable,
        String index,
        DiscoveryNodes discoveryNodes,
        String segmentRepoName,
        String tlogRepoName
    ) {
        Settings currentIndexSettings = indexMetadata.getSettings();
        if (needsRemoteIndexSettingsUpdate(routingTable.indicesRouting().get(index), discoveryNodes, currentIndexSettings)) {
            logger.info(
                "Index {} does not have remote store based index settings but all primary shards and STARTED replica shards have moved to remote enabled nodes. Applying remote store settings to the index",
                index
            );
            assert Objects.nonNull(segmentRepoName) && Objects.nonNull(tlogRepoName) : "Remote repo names cannot be null";
            Settings.Builder indexSettingsBuilder = Settings.builder().put(currentIndexSettings);
            updateRemoteStoreSettings(indexSettingsBuilder, segmentRepoName, tlogRepoName);
            indexMetadataBuilder.settings(indexSettingsBuilder);
            indexMetadataBuilder.settingsVersion(1 + indexMetadata.getVersion());
        } else {
            logger.debug("Index {} does not satisfy criteria for applying remote store settings", index);
        }
    }

    /**
     * Returns <code>true</code> iff all the below conditions are true:
     * - All primary shards are in {@link ShardRoutingState#STARTED} state and are in remote store enabled nodes
     * - No replica shard in {@link ShardRoutingState#RELOCATING} state
     * - All {@link ShardRoutingState#STARTED} replica shards are in remote store enabled nodes
     *
     * @param indexRoutingTable    current {@link IndexRoutingTable} from cluster state
     * @param discoveryNodes       set of discovery nodes from cluster state
     * @param currentIndexSettings current {@link IndexMetadata} from cluster state
     * @return <code>true</code> or <code>false</code> depending on the met conditions
     */
    public boolean needsRemoteIndexSettingsUpdate(
        IndexRoutingTable indexRoutingTable,
        DiscoveryNodes discoveryNodes,
        Settings currentIndexSettings
    ) {
        assert currentIndexSettings != null : "IndexMetadata for a shard cannot be null";
        if (indexHasRemoteStoreSettings(currentIndexSettings) == false) {
            boolean allPrimariesStartedAndOnRemote = indexRoutingTable.shardsMatchingPredicate(ShardRouting::primary)
                .stream()
                .allMatch(shardRouting -> shardRouting.started() && discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
            List<ShardRouting> replicaShards = indexRoutingTable.shardsMatchingPredicate(shardRouting -> shardRouting.primary() == false);
            boolean noRelocatingReplicas = replicaShards.stream().noneMatch(ShardRouting::relocating);
            boolean allStartedReplicasOnRemote = replicaShards.stream()
                .filter(ShardRouting::started)
                .allMatch(shardRouting -> discoveryNodes.get(shardRouting.currentNodeId()).isRemoteStoreNode());
            return allPrimariesStartedAndOnRemote && noRelocatingReplicas && allStartedReplicasOnRemote;
        }
        return false;
    }

    /**
     * Updates the remote store path strategy metadata for the index when it is migrating to remote.
     * This is run during state change of each shard copy when the cluster is in `MIXED` mode and the direction of migration is `REMOTE_STORE`
     * Should not interfere with docrep functionality even if the index is in docrep nodes since this metadata
     * is not used anywhere in the docrep flow
     * Checks are in place to make this execution no-op if the index metadata is already present.
     *
     * @param indexMetadata        Current {@link IndexMetadata}
     * @param indexMetadataBuilder Mutated {@link IndexMetadata.Builder} having the previous state updates
     * @param index                index name
     * @param discoveryNodes       Current {@link DiscoveryNodes} from the cluster state
     * @param settings             current cluster settings from {@link ClusterState}
     */
    public void maybeUpdateRemoteStorePathStrategy(
        IndexMetadata indexMetadata,
        IndexMetadata.Builder indexMetadataBuilder,
        String index,
        DiscoveryNodes discoveryNodes,
        Settings settings
    ) {
        if (indexHasRemotePathMetadata(indexMetadata) == false) {
            logger.info("Adding remote store path strategy for index [{}] during migration", index);
            indexMetadataBuilder.putCustom(REMOTE_STORE_CUSTOM_KEY, createRemoteStorePathTypeMetadata(settings, discoveryNodes));
        } else {
            logger.debug("Index {} already has remote store path strategy", index);
        }
    }

    /**
     * Generates the remote store path type information to be added to custom data of index metadata.
     *
     * @param settings       Current Cluster settings from {@link ClusterState}
     * @param discoveryNodes Current {@link DiscoveryNodes} from the cluster state
     * @return {@link Map} to be added as custom data in index metadata
     */
    public Map<String, String> createRemoteStorePathTypeMetadata(Settings settings, DiscoveryNodes discoveryNodes) {
        Version minNodeVersion = discoveryNodes.getMinNodeVersion();
        PathType pathType = Version.CURRENT.compareTo(minNodeVersion) <= 0
            ? CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.get(settings)
            : PathType.FIXED;
        PathHashAlgorithm pathHashAlgorithm = pathType == PathType.FIXED
            ? null
            : CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING.get(settings);
        Map<String, String> remoteCustomData = new HashMap<>();
        remoteCustomData.put(PathType.NAME, pathType.name());
        if (Objects.nonNull(pathHashAlgorithm)) {
            remoteCustomData.put(PathHashAlgorithm.NAME, pathHashAlgorithm.name());
        }
        return remoteCustomData;
    }

    public static boolean indexHasAllRemoteStoreRelatedMetadata(IndexMetadata indexMetadata) {
        return indexHasRemoteStoreSettings(indexMetadata.getSettings()) && indexHasRemotePathMetadata(indexMetadata);
    }

    /**
     * Assert current index settings have:
     * - index.remote_store.enabled == true
     * - index.remote_store.segment.repository != null
     * - index.remote_store.translog.repository != null
     * - index.replication.type == SEGMENT
     *
     * @param indexSettings Current index settings
     * @return <code>true</code> if all above conditions match. <code>false</code> otherwise
     */
    public static boolean indexHasRemoteStoreSettings(Settings indexSettings) {
        return IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.exists(indexSettings)
            && IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.exists(indexSettings)
            && IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.exists(indexSettings)
            && IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(indexSettings) == ReplicationType.SEGMENT;
    }

    /**
     * Asserts current index metadata customs has the {@link IndexMetadata#REMOTE_STORE_CUSTOM_KEY} key.
     * If it does, checks if the path_type sub-key is present
     *
     * @param indexMetadata Current index metadata
     * @return <code>true</code> if all above conditions match. <code>false</code> otherwise
     */
    public static boolean indexHasRemotePathMetadata(IndexMetadata indexMetadata) {
        Map<String, String> customMetadata = indexMetadata.getCustomData(REMOTE_STORE_CUSTOM_KEY);
        return Objects.nonNull(customMetadata) && Objects.nonNull(customMetadata.get(PathType.NAME));
    }

    public static void updateRemoteStoreSettings(Settings.Builder settingsBuilder, String segmentRepository, String translogRepository) {
        settingsBuilder.put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, segmentRepository)
            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, translogRepository);
    }
}
