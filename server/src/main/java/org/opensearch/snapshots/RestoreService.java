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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.DataStreamMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.cluster.routing.RoutingChangesObserver;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.ArrayUtils;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_HISTORY_UUID;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_UPGRADED;
import static org.opensearch.common.util.FeatureFlags.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY;
import static org.opensearch.common.util.set.Sets.newHashSet;
import static org.opensearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION;
import static org.opensearch.index.store.remote.filecache.FileCache.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.opensearch.node.Node.NODE_SEARCH_CACHE_SIZE_SETTING;
import static org.opensearch.snapshots.SnapshotUtils.filterIndices;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreSnapshotRequest, ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using
 * {@link RoutingTable.Builder#addAsRestore(IndexMetadata, SnapshotRecoverySource)} method.
 * <p>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link IndexShard#restoreFromRepository} )}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link ShardRouting#recoverySource()} property.
 * <p>
 * At the end of the successful restore process {@code RestoreService} calls {@link #cleanupRestoreState(ClusterChangedEvent)},
 * which removes {@link RestoreInProgress} when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 *
 * @opensearch.internal
 */
public class RestoreService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RestoreService.class);

    private static final Set<String> USER_UNMODIFIABLE_SETTINGS = unmodifiableSet(
        newHashSet(
            SETTING_NUMBER_OF_SHARDS,
            SETTING_VERSION_CREATED,
            SETTING_INDEX_UUID,
            SETTING_CREATION_DATE,
            SETTING_HISTORY_UUID,
            SETTING_REMOTE_STORE_ENABLED,
            SETTING_REMOTE_SEGMENT_STORE_REPOSITORY,
            SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY
        )
    );

    // It's OK to change some settings, but we shouldn't allow simply removing them
    private static final Set<String> USER_UNREMOVABLE_SETTINGS;
    private static final String REMOTE_STORE_INDEX_SETTINGS_REGEX = "index.remote_store.*";

    static {
        Set<String> unremovable = new HashSet<>(USER_UNMODIFIABLE_SETTINGS.size() + 4);
        unremovable.addAll(USER_UNMODIFIABLE_SETTINGS);
        unremovable.add(SETTING_NUMBER_OF_REPLICAS);
        unremovable.add(SETTING_AUTO_EXPAND_REPLICAS);
        unremovable.add(SETTING_VERSION_UPGRADED);
        USER_UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
    }

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final AllocationService allocationService;

    private final MetadataCreateIndexService createIndexService;

    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    private final ShardLimitValidator shardLimitValidator;

    private final ClusterSettings clusterSettings;

    private final IndicesService indicesService;

    private final Supplier<ClusterInfo> clusterInfoSupplier;

    private final ClusterManagerTaskThrottler.ThrottlingKey restoreSnapshotTaskKey;

    private static final CleanRestoreStateTaskExecutor cleanRestoreStateTaskExecutor = new CleanRestoreStateTaskExecutor();

    public RestoreService(
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        AllocationService allocationService,
        MetadataCreateIndexService createIndexService,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        ShardLimitValidator shardLimitValidator,
        IndicesService indicesService,
        Supplier<ClusterInfo> clusterInfoSupplier
    ) {
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            clusterService.addStateApplier(this);
        }
        this.clusterSettings = clusterService.getClusterSettings();
        this.shardLimitValidator = shardLimitValidator;
        this.indicesService = indicesService;
        this.clusterInfoSupplier = clusterInfoSupplier;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        restoreSnapshotTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.RESTORE_SNAPSHOT_KEY, true);
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restoreSnapshot(final RestoreSnapshotRequest request, final ActionListener<RestoreCompletionResponse> listener) {
        try {
            // Setting INDEX_STORE_TYPE_SETTING as REMOTE_SNAPSHOT is intended to be a system-managed index setting that is configured when
            // restoring a snapshot and should not be manually set by user.
            String storeTypeSetting = request.indexSettings().get(INDEX_STORE_TYPE_SETTING.getKey());
            if (storeTypeSetting != null && storeTypeSetting.equals(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT.toString())) {
                throw new SnapshotRestoreException(
                    request.repository(),
                    request.snapshot(),
                    "cannot restore remote snapshot with index settings \"index.store.type\" set to \"remote_snapshot\". Instead use \"storage_type\": \"remote_snapshot\" as argument to restore."
                );
            }
            // Read snapshot info and metadata from the repository
            final String repositoryName = request.repository();
            Repository repository = repositoriesService.repository(repositoryName);
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            repository.getRepositoryData(repositoryDataListener);
            repositoryDataListener.whenComplete(repositoryData -> {
                final String snapshotName = request.snapshot();
                final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds()
                    .stream()
                    .filter(s -> snapshotName.equals(s.getName()))
                    .findFirst();
                if (matchingSnapshotId.isPresent() == false) {
                    throw new SnapshotRestoreException(repositoryName, snapshotName, "snapshot does not exist");
                }

                final SnapshotId snapshotId = matchingSnapshotId.get();
                if (request.snapshotUuid() != null && request.snapshotUuid().equals(snapshotId.getUUID()) == false) {
                    throw new SnapshotRestoreException(
                        repositoryName,
                        snapshotName,
                        "snapshot UUID mismatch: expected [" + request.snapshotUuid() + "] but got [" + snapshotId.getUUID() + "]"
                    );
                }

                final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

                // Make sure that we can restore from this snapshot
                validateSnapshotRestorable(repositoryName, snapshotInfo);

                Metadata globalMetadata = null;
                // Resolve the indices from the snapshot that need to be restored
                Map<String, DataStream> dataStreams;
                List<String> requestIndices = new ArrayList<>(Arrays.asList(request.indices()));

                List<String> requestedDataStreams = filterIndices(
                    snapshotInfo.dataStreams(),
                    requestIndices.toArray(new String[0]),
                    IndicesOptions.fromOptions(true, true, true, true)
                );
                if (requestedDataStreams.isEmpty()) {
                    dataStreams = new HashMap<>();
                } else {
                    globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
                    final Map<String, DataStream> dataStreamsInSnapshot = globalMetadata.dataStreams();
                    dataStreams = new HashMap<>(requestedDataStreams.size());
                    for (String requestedDataStream : requestedDataStreams) {
                        final DataStream dataStreamInSnapshot = dataStreamsInSnapshot.get(requestedDataStream);
                        assert dataStreamInSnapshot != null : "DataStream [" + requestedDataStream + "] not found in snapshot";
                        dataStreams.put(requestedDataStream, dataStreamInSnapshot);
                    }
                }
                requestIndices.removeAll(dataStreams.keySet());
                Set<String> dataStreamIndices = dataStreams.values()
                    .stream()
                    .flatMap(ds -> ds.getIndices().stream())
                    .map(Index::getName)
                    .collect(Collectors.toSet());
                requestIndices.addAll(dataStreamIndices);

                final List<String> indicesInSnapshot = filterIndices(
                    snapshotInfo.indices(),
                    requestIndices.toArray(new String[0]),
                    request.indicesOptions()
                );

                final Metadata.Builder metadataBuilder;
                if (request.includeGlobalState()) {
                    if (globalMetadata == null) {
                        globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
                    }
                    metadataBuilder = Metadata.builder(globalMetadata);
                } else {
                    metadataBuilder = Metadata.builder();
                }

                final List<IndexId> indexIdsInSnapshot = repositoryData.resolveIndices(indicesInSnapshot);
                for (IndexId indexId : indexIdsInSnapshot) {
                    metadataBuilder.put(repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId), false);
                }

                final Metadata metadata = metadataBuilder.build();

                // Apply renaming on index names, returning a map of names where
                // the key is the renamed index and the value is the original name
                final Map<String, String> indices = renamedIndices(request, indicesInSnapshot, dataStreamIndices);

                // Now we can start the actual restore process by adding shards to be recovered in the cluster state
                // and updating cluster metadata (global and index) as needed
                clusterService.submitStateUpdateTask("restore_snapshot[" + snapshotName + ']', new ClusterStateUpdateTask() {
                    final String restoreUUID = UUIDs.randomBase64UUID();
                    RestoreInfo restoreInfo = null;

                    @Override
                    public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                        return restoreSnapshotTaskKey;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
                        if (currentState.getNodes().getMinNodeVersion().before(LegacyESVersion.V_7_0_0)) {
                            // Check if another restore process is already running - cannot run two restore processes at the
                            // same time in versions prior to 7.0
                            if (restoreInProgress.isEmpty() == false) {
                                throw new ConcurrentSnapshotExecutionException(
                                    snapshot,
                                    "Restore process is already running in this cluster"
                                );
                            }
                        }
                        // Check if the snapshot to restore is currently being deleted
                        SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                            SnapshotDeletionsInProgress.TYPE,
                            SnapshotDeletionsInProgress.EMPTY
                        );
                        if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(snapshotId))) {
                            throw new ConcurrentSnapshotExecutionException(
                                snapshot,
                                "cannot restore a snapshot while a snapshot deletion is in-progress ["
                                    + deletionsInProgress.getEntries().get(0)
                                    + "]"
                            );
                        }

                        // Updating cluster state
                        ClusterState.Builder builder = ClusterState.builder(currentState);
                        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards;
                        final boolean isRemoteSnapshot = IndexModule.Type.REMOTE_SNAPSHOT.match(request.storageType().toString());
                        Set<String> aliases = new HashSet<>();
                        long totalRestorableRemoteIndexesSize = 0;

                        if (indices.isEmpty() == false) {
                            // We have some indices to restore
                            Map<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = new HashMap<>();

                            for (Map.Entry<String, String> indexEntry : indices.entrySet()) {
                                String renamedIndexName = indexEntry.getKey();
                                String index = indexEntry.getValue();
                                boolean partial = checkPartial(index);

                                IndexId snapshotIndexId = repositoryData.resolveIndexId(index);

                                final Settings overrideSettingsInternal = getOverrideSettingsInternal();
                                final String[] ignoreSettingsInternal = getIgnoreSettingsInternal();

                                IndexMetadata snapshotIndexMetadata = updateIndexSettings(
                                    metadata.index(index),
                                    request.indexSettings(),
                                    request.ignoreIndexSettings(),
                                    overrideSettingsInternal,
                                    ignoreSettingsInternal
                                );
                                if (isRemoteSnapshot) {
                                    snapshotIndexMetadata = addSnapshotToIndexSettings(snapshotIndexMetadata, snapshot, snapshotIndexId);
                                }
                                final boolean isSearchableSnapshot = IndexModule.Type.REMOTE_SNAPSHOT.match(
                                    snapshotIndexMetadata.getSettings().get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
                                );
                                final boolean isRemoteStoreShallowCopy = Boolean.TRUE.equals(
                                    snapshotInfo.isRemoteStoreIndexShallowCopyEnabled()
                                ) && metadata.index(index).getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false);
                                if (isSearchableSnapshot && isRemoteStoreShallowCopy) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "Shallow copy snapshot cannot be restored as searchable snapshot."
                                    );
                                }
                                if (isRemoteStoreShallowCopy && !currentState.getNodes().getMinNodeVersion().onOrAfter(Version.V_2_9_0)) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "cannot restore shallow copy snapshot for index ["
                                            + index
                                            + "] as some of the nodes in cluster have version less than 2.9"
                                    );
                                }
                                final SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(
                                    restoreUUID,
                                    snapshot,
                                    snapshotInfo.version(),
                                    snapshotIndexId,
                                    isSearchableSnapshot,
                                    isRemoteStoreShallowCopy,
                                    request.getSourceRemoteStoreRepository()
                                );
                                final Version minIndexCompatibilityVersion;
                                if (isSearchableSnapshot && isSearchableSnapshotsExtendedCompatibilityEnabled()) {
                                    minIndexCompatibilityVersion = SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION
                                        .minimumIndexCompatibilityVersion();
                                } else {
                                    minIndexCompatibilityVersion = currentState.getNodes()
                                        .getMaxNodeVersion()
                                        .minimumIndexCompatibilityVersion();
                                }
                                try {
                                    snapshotIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(
                                        snapshotIndexMetadata,
                                        minIndexCompatibilityVersion
                                    );
                                } catch (Exception ex) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "cannot restore index [" + index + "] because it cannot be upgraded",
                                        ex
                                    );
                                }
                                // Check that the index is closed or doesn't exist
                                IndexMetadata currentIndexMetadata = currentState.metadata().index(renamedIndexName);
                                Set<Integer> ignoreShards = new HashSet<>();
                                final Index renamedIndex;
                                if (currentIndexMetadata == null) {
                                    // Index doesn't exist - create it and start recovery
                                    // Make sure that the index we are about to create has valid name
                                    boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(snapshotIndexMetadata.getSettings());
                                    createIndexService.validateIndexName(renamedIndexName, currentState);
                                    createIndexService.validateDotIndex(renamedIndexName, isHidden);
                                    createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);
                                    MetadataCreateIndexService.validateRefreshIntervalSettings(
                                        snapshotIndexMetadata.getSettings(),
                                        clusterSettings
                                    );
                                    IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                                        .state(IndexMetadata.State.OPEN)
                                        .index(renamedIndexName);
                                    indexMdBuilder.settings(
                                        Settings.builder()
                                            .put(snapshotIndexMetadata.getSettings())
                                            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                                    );
                                    createIndexService.addRemoteCustomData(indexMdBuilder);
                                    shardLimitValidator.validateShardLimit(
                                        renamedIndexName,
                                        snapshotIndexMetadata.getSettings(),
                                        currentState
                                    );
                                    if (!request.includeAliases() && !snapshotIndexMetadata.getAliases().isEmpty()) {
                                        // Remove all aliases - they shouldn't be restored
                                        indexMdBuilder.removeAllAliases();
                                    } else {
                                        for (final String alias : snapshotIndexMetadata.getAliases().keySet()) {
                                            aliases.add(alias);
                                        }
                                    }
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.build();
                                    if (partial) {
                                        populateIgnoredShards(index, ignoreShards);
                                    }
                                    rtBuilder.addAsNewRestore(updatedIndexMetadata, recoverySource, ignoreShards);
                                    blocks.addBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                } else {
                                    validateExistingIndex(currentIndexMetadata, snapshotIndexMetadata, renamedIndexName, partial);
                                    // Index exists and it's closed - open it in metadata and start recovery
                                    IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                                        .state(IndexMetadata.State.OPEN);
                                    indexMdBuilder.version(
                                        Math.max(snapshotIndexMetadata.getVersion(), 1 + currentIndexMetadata.getVersion())
                                    );
                                    indexMdBuilder.mappingVersion(
                                        Math.max(snapshotIndexMetadata.getMappingVersion(), 1 + currentIndexMetadata.getMappingVersion())
                                    );
                                    indexMdBuilder.settingsVersion(
                                        Math.max(snapshotIndexMetadata.getSettingsVersion(), 1 + currentIndexMetadata.getSettingsVersion())
                                    );
                                    indexMdBuilder.aliasesVersion(
                                        Math.max(snapshotIndexMetadata.getAliasesVersion(), 1 + currentIndexMetadata.getAliasesVersion())
                                    );

                                    for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                        indexMdBuilder.primaryTerm(
                                            shard,
                                            Math.max(snapshotIndexMetadata.primaryTerm(shard), currentIndexMetadata.primaryTerm(shard))
                                        );
                                    }

                                    if (!request.includeAliases()) {
                                        // Remove all snapshot aliases
                                        if (!snapshotIndexMetadata.getAliases().isEmpty()) {
                                            indexMdBuilder.removeAllAliases();
                                        }
                                        /// Add existing aliases
                                        for (final AliasMetadata alias : currentIndexMetadata.getAliases().values()) {
                                            indexMdBuilder.putAlias(alias);
                                        }
                                    } else {
                                        for (final String alias : snapshotIndexMetadata.getAliases().keySet()) {
                                            aliases.add(alias);
                                        }
                                    }
                                    final Settings.Builder indexSettingsBuilder = Settings.builder()
                                        .put(snapshotIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_INDEX_UUID, currentIndexMetadata.getIndexUUID());
                                    // Only add a restore uuid if either all nodes in the cluster support it (version >= 7.9) or if the
                                    // index itself was created after 7.9 and thus won't be restored to a node that doesn't support the
                                    // setting anyway
                                    if (snapshotIndexMetadata.getCreationVersion().onOrAfter(LegacyESVersion.V_7_9_0)
                                        || currentState.nodes().getMinNodeVersion().onOrAfter(LegacyESVersion.V_7_9_0)) {
                                        indexSettingsBuilder.put(SETTING_HISTORY_UUID, UUIDs.randomBase64UUID());
                                    }
                                    indexMdBuilder.settings(indexSettingsBuilder);
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.index(renamedIndexName).build();
                                    rtBuilder.addAsRestore(updatedIndexMetadata, recoverySource);
                                    blocks.updateBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                }

                                for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                    if (isRemoteSnapshot) {
                                        IndexShardSnapshotStatus.Copy shardStatus = repository.getShardSnapshotStatus(
                                            snapshotInfo.snapshotId(),
                                            snapshotIndexId,
                                            new ShardId(metadata.index(index).getIndex(), shard)
                                        ).asCopy();
                                        totalRestorableRemoteIndexesSize += shardStatus.getTotalSize();
                                    }
                                    if (!ignoreShards.contains(shard)) {
                                        shardsBuilder.put(
                                            new ShardId(renamedIndex, shard),
                                            new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId())
                                        );
                                    } else {
                                        shardsBuilder.put(
                                            new ShardId(renamedIndex, shard),
                                            new RestoreInProgress.ShardRestoreStatus(
                                                clusterService.state().nodes().getLocalNodeId(),
                                                RestoreInProgress.State.FAILURE
                                            )
                                        );
                                    }
                                }
                            }

                            shards = Collections.unmodifiableMap(shardsBuilder);
                            RestoreInProgress.Entry restoreEntry = new RestoreInProgress.Entry(
                                restoreUUID,
                                snapshot,
                                overallState(RestoreInProgress.State.INIT, shards),
                                Collections.unmodifiableList(new ArrayList<>(indices.keySet())),
                                shards
                            );
                            builder.putCustom(
                                RestoreInProgress.TYPE,
                                new RestoreInProgress.Builder(currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).add(
                                    restoreEntry
                                ).build()
                            );
                        } else {
                            shards = Map.of();
                        }

                        checkAliasNameConflicts(indices, aliases);
                        if (isRemoteSnapshot) {
                            validateSearchableSnapshotRestorable(totalRestorableRemoteIndexesSize);
                        }

                        Map<String, DataStream> updatedDataStreams = new HashMap<>(currentState.metadata().dataStreams());
                        updatedDataStreams.putAll(
                            dataStreams.values()
                                .stream()
                                .map(ds -> updateDataStream(ds, mdBuilder, request))
                                .collect(Collectors.toMap(DataStream::getName, Function.identity()))
                        );
                        mdBuilder.dataStreams(updatedDataStreams);

                        // Restore global state if needed
                        if (request.includeGlobalState()) {
                            if (metadata.persistentSettings() != null) {
                                Settings settings = metadata.persistentSettings();
                                clusterSettings.validateUpdate(settings);
                                mdBuilder.persistentSettings(settings);
                            }
                            if (metadata.templates() != null) {
                                // TODO: Should all existing templates be deleted first?
                                for (final IndexTemplateMetadata cursor : metadata.templates().values()) {
                                    MetadataCreateIndexService.validateRefreshIntervalSettings(cursor.settings(), clusterSettings);
                                    mdBuilder.put(cursor);
                                }
                            }
                            if (metadata.customs() != null) {
                                for (final Map.Entry<String, Metadata.Custom> cursor : metadata.customs().entrySet()) {
                                    if (RepositoriesMetadata.TYPE.equals(cursor.getKey()) == false
                                        && DataStreamMetadata.TYPE.equals(cursor.getKey()) == false) {
                                        // Don't restore repositories while we are working with them
                                        // TODO: Should we restore them at the end?
                                        // Also, don't restore data streams here, we already added them to the metadata builder above
                                        mdBuilder.putCustom(cursor.getKey(), cursor.getValue());
                                    }
                                }
                            }
                        }

                        if (completed(shards)) {
                            // We don't have any indices to restore - we are done
                            restoreInfo = new RestoreInfo(
                                snapshotId.getName(),
                                Collections.unmodifiableList(new ArrayList<>(indices.keySet())),
                                shards.size(),
                                shards.size() - failedShards(shards)
                            );
                        }

                        RoutingTable rt = rtBuilder.build();
                        ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
                        return allocationService.reroute(updatedState, "restored snapshot [" + snapshot + "]");
                    }

                    private void checkAliasNameConflicts(Map<String, String> renamedIndices, Set<String> aliases) {
                        for (Map.Entry<String, String> renamedIndex : renamedIndices.entrySet()) {
                            if (aliases.contains(renamedIndex.getKey())) {
                                throw new SnapshotRestoreException(
                                    snapshot,
                                    "cannot rename index ["
                                        + renamedIndex.getValue()
                                        + "] into ["
                                        + renamedIndex.getKey()
                                        + "] because of conflict with an alias with the same name"
                                );
                            }
                        }
                    }

                    private String[] getIgnoreSettingsInternal() {
                        // for non-remote store enabled domain, we will remove all the remote store
                        // related index settings present in the snapshot.
                        String[] indexSettingsToBeIgnored = new String[] {};
                        if (false == RemoteStoreNodeAttribute.isRemoteStoreAttributePresent(clusterService.getSettings())) {
                            indexSettingsToBeIgnored = ArrayUtils.concat(
                                indexSettingsToBeIgnored,
                                new String[] { REMOTE_STORE_INDEX_SETTINGS_REGEX }
                            );
                        }
                        return indexSettingsToBeIgnored;
                    }

                    private Settings getOverrideSettingsInternal() {
                        final Settings.Builder settingsBuilder = Settings.builder();

                        // We will use whatever replication strategy provided by user or from snapshot metadata unless
                        // cluster is remote store enabled or user have restricted a specific replication type in the
                        // cluster. If cluster is undergoing remote store migration, replication strategy is strictly SEGMENT type
                        if (RemoteStoreNodeAttribute.isRemoteStoreAttributePresent(clusterService.getSettings())
                            || clusterSettings.get(IndicesService.CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING)
                            || RemoteStoreNodeService.isMigratingToRemoteStore(clusterSettings)) {
                            MetadataCreateIndexService.updateReplicationStrategy(
                                settingsBuilder,
                                request.indexSettings(),
                                clusterService.getSettings(),
                                null,
                                clusterSettings
                            );
                        }
                        // remote store settings needs to be overridden if the remote store feature is enabled in the
                        // cluster where snapshot is being restored.
                        MetadataCreateIndexService.updateRemoteStoreSettings(
                            settingsBuilder,
                            clusterService.state(),
                            clusterSettings,
                            clusterService.getSettings(),
                            request.getDescription()
                        );
                        return settingsBuilder.build();
                    }

                    private void populateIgnoredShards(String index, final Set<Integer> ignoreShards) {
                        for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
                            if (index.equals(failure.index())) {
                                ignoreShards.add(failure.shardId());
                            }
                        }
                    }

                    private boolean checkPartial(String index) {
                        // Make sure that index was fully snapshotted
                        if (failed(snapshotInfo, index)) {
                            if (request.partial()) {
                                return true;
                            } else {
                                throw new SnapshotRestoreException(
                                    snapshot,
                                    "index [" + index + "] wasn't fully snapshotted - cannot " + "restore"
                                );
                            }
                        } else {
                            return false;
                        }
                    }

                    private void validateExistingIndex(
                        IndexMetadata currentIndexMetadata,
                        IndexMetadata snapshotIndexMetadata,
                        String renamedIndex,
                        boolean partial
                    ) {
                        // Index exist - checking that it's closed
                        if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                            // TODO: Enable restore for open indices
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot restore index ["
                                    + renamedIndex
                                    + "] because an open index "
                                    + "with same name already exists in the cluster. Either close or delete the existing index or restore the "
                                    + "index under a different name by providing a rename pattern and replacement name"
                            );
                        }
                        // Index exist - checking if it's partial restore
                        if (partial) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot restore partial index [" + renamedIndex + "] because such index already exists"
                            );
                        }
                        // Make sure that the number of shards is the same. That's the only thing that we cannot change
                        if (currentIndexMetadata.getNumberOfShards() != snapshotIndexMetadata.getNumberOfShards()) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot restore index ["
                                    + renamedIndex
                                    + "] with ["
                                    + currentIndexMetadata.getNumberOfShards()
                                    + "] shards from a snapshot of index ["
                                    + snapshotIndexMetadata.getIndex().getName()
                                    + "] with ["
                                    + snapshotIndexMetadata.getNumberOfShards()
                                    + "] shards"
                            );
                        }
                    }

                    /**
                     * Optionally updates index settings in indexMetadata by removing settings listed in ignoreSettings and
                     * merging them with settings in changeSettings.
                     */
                    private IndexMetadata updateIndexSettings(
                        IndexMetadata indexMetadata,
                        Settings overrideSettings,
                        String[] ignoreSettings,
                        Settings overrideSettingsInternal,
                        String[] ignoreSettingsInternal
                    ) {
                        Settings normalizedChangeSettings = Settings.builder()
                            .put(overrideSettings)
                            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
                            .build();
                        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings())
                            && IndexSettings.INDEX_SOFT_DELETES_SETTING.exists(overrideSettings)
                            && IndexSettings.INDEX_SOFT_DELETES_SETTING.get(overrideSettings) == false) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot disable setting [" + IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey() + "] on restore"
                            );
                        }
                        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
                        Settings settings = indexMetadata.getSettings();
                        Set<String> keyFilters = new HashSet<>();
                        List<String> simpleMatchPatterns = new ArrayList<>();
                        for (String ignoredSetting : ignoreSettings) {
                            if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                                if (USER_UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "cannot remove setting [" + ignoredSetting + "] on restore"
                                    );
                                } else {
                                    keyFilters.add(ignoredSetting);
                                }
                            } else {
                                simpleMatchPatterns.add(ignoredSetting);
                            }
                        }

                        // add internal settings to ignore settings list
                        for (String ignoredSetting : ignoreSettingsInternal) {
                            if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                                keyFilters.add(ignoredSetting);
                            } else {
                                simpleMatchPatterns.add(ignoredSetting);
                            }
                        }

                        Predicate<String> settingsFilter = k -> {
                            if (USER_UNREMOVABLE_SETTINGS.contains(k) == false) {
                                for (String filterKey : keyFilters) {
                                    if (k.equals(filterKey)) {
                                        return false;
                                    }
                                }
                                for (String pattern : simpleMatchPatterns) {
                                    if (Regex.simpleMatch(pattern, k)) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        };
                        Settings.Builder settingsBuilder = Settings.builder()
                            .put(settings.filter(settingsFilter))
                            .put(normalizedChangeSettings.filter(k -> {
                                if (USER_UNMODIFIABLE_SETTINGS.contains(k)) {
                                    throw new SnapshotRestoreException(snapshot, "cannot modify setting [" + k + "] on restore");
                                } else {
                                    return true;
                                }
                            }));

                        // override internal settings
                        if (overrideSettingsInternal != null) {
                            settingsBuilder.put(overrideSettingsInternal).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
                        }
                        settingsBuilder.remove(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey());
                        return builder.settings(settingsBuilder).build();
                    }

                    private void validateSearchableSnapshotRestorable(long totalRestorableRemoteIndexesSize) {
                        ClusterInfo clusterInfo = clusterInfoSupplier.get();
                        double remoteDataToFileCacheRatio = DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.get(clusterService.getSettings());
                        Map<String, FileCacheStats> nodeFileCacheStats = clusterInfo.getNodeFileCacheStats();
                        if (nodeFileCacheStats.isEmpty() || remoteDataToFileCacheRatio <= 0.01f) {
                            return;
                        }

                        long totalNodeFileCacheSize = clusterInfo.getNodeFileCacheStats()
                            .values()
                            .stream()
                            .map(fileCacheStats -> fileCacheStats.getTotal().getBytes())
                            .mapToLong(Long::longValue)
                            .sum();

                        Predicate<ShardRouting> isRemoteSnapshotShard = shardRouting -> shardRouting.primary()
                            && indicesService.indexService(shardRouting.index()).getIndexSettings().isRemoteSnapshot();

                        ShardsIterator shardsIterator = clusterService.state()
                            .routingTable()
                            .allShardsSatisfyingPredicate(isRemoteSnapshotShard);

                        long totalRestoredRemoteIndexesSize = shardsIterator.getShardRoutings()
                            .stream()
                            .map(clusterInfo::getShardSize)
                            .mapToLong(Long::longValue)
                            .sum();

                        if (totalRestoredRemoteIndexesSize + totalRestorableRemoteIndexesSize > remoteDataToFileCacheRatio
                            * totalNodeFileCacheSize) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "Size of the indexes to be restored exceeds the file cache bounds. Increase the file cache capacity on the cluster nodes using "
                                    + NODE_SEARCH_CACHE_SIZE_SETTING.getKey()
                                    + " setting."
                            );
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot", snapshotId), e);
                        listener.onFailure(e);
                    }

                    @Override
                    public TimeValue timeout() {
                        return request.clusterManagerNodeTimeout();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new RestoreCompletionResponse(restoreUUID, snapshot, restoreInfo));
                    }
                });
            }, listener::onFailure);
        } catch (Exception e) {
            logger.warn(
                () -> new ParameterizedMessage("[{}] failed to restore snapshot", request.repository() + ":" + request.snapshot()),
                e
            );
            listener.onFailure(e);
        }
    }

    // visible for testing
    static DataStream updateDataStream(DataStream dataStream, Metadata.Builder metadata, RestoreSnapshotRequest request) {
        String dataStreamName = dataStream.getName();
        if (request.renamePattern() != null && request.renameReplacement() != null) {
            dataStreamName = dataStreamName.replaceAll(request.renamePattern(), request.renameReplacement());
        }
        List<Index> updatedIndices = dataStream.getIndices()
            .stream()
            .map(i -> metadata.get(renameIndex(i.getName(), request, true)).getIndex())
            .collect(Collectors.toList());
        return new DataStream(dataStreamName, dataStream.getTimeStampField(), updatedIndices, dataStream.getGeneration());
    }

    public static RestoreInProgress updateRestoreStateWithDeletedIndices(RestoreInProgress oldRestore, Set<Index> deletedIndices) {
        boolean changesMade = false;
        RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
        for (RestoreInProgress.Entry entry : oldRestore) {
            Map<ShardId, ShardRestoreStatus> shardsBuilder = null;
            for (final Map.Entry<ShardId, ShardRestoreStatus> cursor : entry.shards().entrySet()) {
                ShardId shardId = cursor.getKey();
                if (deletedIndices.contains(shardId.getIndex())) {
                    changesMade = true;
                    if (shardsBuilder == null) {
                        shardsBuilder = new HashMap<>(entry.shards());
                    }
                    shardsBuilder.put(shardId, new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "index was deleted"));
                }
            }
            if (shardsBuilder != null) {
                final Map<ShardId, ShardRestoreStatus> shards = Collections.unmodifiableMap(shardsBuilder);
                builder.add(
                    new RestoreInProgress.Entry(
                        entry.uuid(),
                        entry.snapshot(),
                        overallState(RestoreInProgress.State.STARTED, shards),
                        entry.indices(),
                        shards
                    )
                );
            } else {
                builder.add(entry);
            }
        }
        if (changesMade) {
            return builder.build();
        } else {
            return oldRestore;
        }
    }

    /**
     * Response once restore is completed.
     *
     * @opensearch.internal
     */
    public static final class RestoreCompletionResponse {
        private final String uuid;
        private final Snapshot snapshot;
        private final RestoreInfo restoreInfo;

        public RestoreCompletionResponse(final String uuid, final Snapshot snapshot, final RestoreInfo restoreInfo) {
            this.uuid = uuid;
            this.snapshot = snapshot;
            this.restoreInfo = restoreInfo;
        }

        public String getUuid() {
            return uuid;
        }

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

    /**
     * Updates based on restore progress
     *
     * @opensearch.internal
     */
    public static class RestoreInProgressUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
        // Map of RestoreUUID to a of changes to the shards' restore statuses
        private final Map<String, Map<ShardId, ShardRestoreStatus>> shardChanges = new HashMap<>();

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            // mark snapshot as completed
            if (initializingShard.primary()) {
                RecoverySource recoverySource = initializingShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    changes(recoverySource).put(
                        initializingShard.shardId(),
                        new ShardRestoreStatus(initializingShard.currentNodeId(), RestoreInProgress.State.SUCCESS)
                    );
                }
            }
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            if (failedShard.primary() && failedShard.initializing()) {
                RecoverySource recoverySource = failedShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    // mark restore entry for this shard as failed when it's due to a file corruption. There is no need wait on retries
                    // to restore this shard on another node if the snapshot files are corrupt. In case where a node just left or crashed,
                    // however, we only want to acknowledge the restore operation once it has been successfully restored on another node.
                    if (unassignedInfo.getFailure() != null && Lucene.isCorruptionException(unassignedInfo.getFailure().getCause())) {
                        changes(recoverySource).put(
                            failedShard.shardId(),
                            new ShardRestoreStatus(
                                failedShard.currentNodeId(),
                                RestoreInProgress.State.FAILURE,
                                unassignedInfo.getFailure().getCause().getMessage()
                            )
                        );
                    }
                }
            }
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            // if we force an empty primary, we should also fail the restore entry
            if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
                && initializedShard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                changes(unassignedShard.recoverySource()).put(
                    unassignedShard.shardId(),
                    new ShardRestoreStatus(
                        null,
                        RestoreInProgress.State.FAILURE,
                        "recovery source type changed from snapshot to " + initializedShard.recoverySource()
                    )
                );
            }
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            RecoverySource recoverySource = unassignedShard.recoverySource();
            if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                if (newUnassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
                    String reason = "shard could not be allocated to any of the nodes";
                    changes(recoverySource).put(
                        unassignedShard.shardId(),
                        new ShardRestoreStatus(unassignedShard.currentNodeId(), RestoreInProgress.State.FAILURE, reason)
                    );
                }
            }
        }

        /**
         * Helper method that creates update entry for the given recovery source's restore uuid
         * if such an entry does not exist yet.
         */
        private Map<ShardId, ShardRestoreStatus> changes(RecoverySource recoverySource) {
            assert recoverySource.getType() == RecoverySource.Type.SNAPSHOT;
            return shardChanges.computeIfAbsent(((SnapshotRecoverySource) recoverySource).restoreUUID(), k -> new HashMap<>());
        }

        public RestoreInProgress applyChanges(final RestoreInProgress oldRestore) {
            if (shardChanges.isEmpty() == false) {
                RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
                for (RestoreInProgress.Entry entry : oldRestore) {
                    Map<ShardId, ShardRestoreStatus> updates = shardChanges.get(entry.uuid());
                    final Map<ShardId, ShardRestoreStatus> shardStates = entry.shards();
                    if (updates != null && updates.isEmpty() == false) {
                        Map<ShardId, ShardRestoreStatus> shardsBuilder = new HashMap<>(shardStates);
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.entrySet()) {
                            ShardId shardId = shard.getKey();
                            ShardRestoreStatus status = shardStates.get(shardId);
                            if (status == null || status.state().completed() == false) {
                                shardsBuilder.put(shardId, shard.getValue());
                            }
                        }

                        final Map<ShardId, ShardRestoreStatus> shards = Collections.unmodifiableMap(shardsBuilder);
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), newState, entry.indices(), shards));
                    } else {
                        builder.add(entry);
                    }
                }
                return builder.build();
            } else {
                return oldRestore;
            }
        }
    }

    public static RestoreInProgress.Entry restoreInProgress(ClusterState state, String restoreUUID) {
        return state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).get(restoreUUID);
    }

    static class CleanRestoreStateTaskExecutor
        implements
            ClusterStateTaskExecutor<CleanRestoreStateTaskExecutor.Task>,
            ClusterStateTaskListener {

        static class Task {
            final String uuid;

            Task(String uuid) {
                this.uuid = uuid;
            }

            @Override
            public String toString() {
                return "clean restore state for restore " + uuid;
            }
        }

        @Override
        public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) {
            final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
            Set<String> completedRestores = tasks.stream().map(e -> e.uuid).collect(Collectors.toSet());
            RestoreInProgress.Builder restoreInProgressBuilder = new RestoreInProgress.Builder();
            boolean changed = false;
            for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
                if (completedRestores.contains(entry.uuid())) {
                    changed = true;
                } else {
                    restoreInProgressBuilder.add(entry);
                }
            }
            if (changed == false) {
                return resultBuilder.build(currentState);
            }
            final Map<String, ClusterState.Custom> builder = new HashMap<>(currentState.getCustoms());
            builder.put(RestoreInProgress.TYPE, restoreInProgressBuilder.build());
            final Map<String, ClusterState.Custom> customs = Collections.unmodifiableMap(builder);
            return resultBuilder.build(ClusterState.builder(currentState).customs(customs).build());
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }

        @Override
        public void onNoLongerClusterManager(String source) {
            logger.debug("no longer cluster-manager while processing restore state update [{}]", source);
        }

    }

    private void cleanupRestoreState(ClusterChangedEvent event) {
        for (RestoreInProgress.Entry entry : event.state().custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            if (entry.state().completed()) {
                assert completed(entry.shards()) : "state says completed but restore entries are not";
                clusterService.submitStateUpdateTask(
                    "clean up snapshot restore state",
                    new CleanRestoreStateTaskExecutor.Task(entry.uuid()),
                    ClusterStateTaskConfig.build(Priority.URGENT),
                    cleanRestoreStateTaskExecutor,
                    cleanRestoreStateTaskExecutor
                );
            }
        }
    }

    private static RestoreInProgress.State overallState(
        RestoreInProgress.State nonCompletedState,
        final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards
    ) {
        boolean hasFailed = false;
        for (RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state().completed() == false) {
                return nonCompletedState;
            }
            if (status.state() == RestoreInProgress.State.FAILURE) {
                hasFailed = true;
            }
        }
        if (hasFailed) {
            return RestoreInProgress.State.FAILURE;
        } else {
            return RestoreInProgress.State.SUCCESS;
        }
    }

    public static boolean completed(final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (final RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state().completed() == false) {
                return false;
            }
        }
        return true;
    }

    public static int failedShards(final Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (final RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state() == RestoreInProgress.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    private static Map<String, String> renamedIndices(
        RestoreSnapshotRequest request,
        List<String> filteredIndices,
        Set<String> dataStreamIndices
    ) {
        Map<String, String> renamedIndices = new HashMap<>();
        for (String index : filteredIndices) {
            String renamedIndex = renameIndex(index, request, dataStreamIndices.contains(index));
            String previousIndex = renamedIndices.put(renamedIndex, index);
            if (previousIndex != null) {
                throw new SnapshotRestoreException(
                    request.repository(),
                    request.snapshot(),
                    "indices [" + index + "] and [" + previousIndex + "] are renamed into the same index [" + renamedIndex + "]"
                );
            }
        }
        return Collections.unmodifiableMap(renamedIndices);
    }

    private static String renameIndex(String index, RestoreSnapshotRequest request, boolean partOfDataStream) {
        String renamedIndex = index;
        if (request.renameReplacement() != null && request.renamePattern() != null) {
            partOfDataStream = partOfDataStream && index.startsWith(DataStream.BACKING_INDEX_PREFIX);
            if (partOfDataStream) {
                index = index.substring(DataStream.BACKING_INDEX_PREFIX.length());
            }
            renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
            if (partOfDataStream) {
                renamedIndex = DataStream.BACKING_INDEX_PREFIX + renamedIndex;
            }
        }
        return renamedIndex;
    }

    /**
     * Checks that snapshots can be restored and have compatible version
     *
     * @param repository      repository name
     * @param snapshotInfo    snapshot metadata
     */
    private static void validateSnapshotRestorable(final String repository, final SnapshotInfo snapshotInfo) {
        if (!snapshotInfo.state().restorable()) {
            throw new SnapshotRestoreException(
                new Snapshot(repository, snapshotInfo.snapshotId()),
                "unsupported snapshot state [" + snapshotInfo.state() + "]"
            );
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(
                new Snapshot(repository, snapshotInfo.snapshotId()),
                "the snapshot was created with OpenSearch version ["
                    + snapshotInfo.version()
                    + "] which is higher than the version of this node ["
                    + Version.CURRENT
                    + "]"
            );
        }
    }

    public static boolean failed(SnapshotInfo snapshot, String index) {
        for (SnapshotShardFailure failure : snapshot.shardFailures()) {
            if (index.equals(failure.index())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the indices that are currently being restored and that are contained in the indices-to-check set.
     */
    public static Set<Index> restoringIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            for (final Map.Entry<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards().entrySet()) {
                Index index = shard.getKey().getIndex();
                if (indicesToCheck.contains(index)
                    && shard.getValue().state().completed() == false
                    && currentState.getMetadata().index(index) != null) {
                    indices.add(index);
                }
            }
        }
        return indices;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeClusterManager()) {
                cleanupRestoreState(event);
            }
        } catch (Exception t) {
            logger.warn("Failed to update restore state ", t);
        }
    }

    private static IndexMetadata addSnapshotToIndexSettings(IndexMetadata metadata, Snapshot snapshot, IndexId indexId) {
        final Settings newSettings = Settings.builder()
            .put(metadata.getSettings())
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.getKey(), snapshot.getRepository())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.getKey(), snapshot.getSnapshotId().getUUID())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.getKey(), snapshot.getSnapshotId().getName())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_INDEX_ID.getKey(), indexId.getId())
            .build();
        return IndexMetadata.builder(metadata).settings(newSettings).build();
    }

    private static boolean isSearchableSnapshotsExtendedCompatibilityEnabled() {
        return org.opensearch.Version.CURRENT.after(org.opensearch.Version.V_2_4_0)
            && FeatureFlags.isEnabled(SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY);
    }
}
