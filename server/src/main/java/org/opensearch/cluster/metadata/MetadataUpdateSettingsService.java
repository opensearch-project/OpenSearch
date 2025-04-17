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

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeSettingsClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.service.ClusterManagerTaskKeys;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.ValidationException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.validateOverlap;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.validateRefreshIntervalSettings;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.validateTranslogDurabilitySettings;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex;
import static org.opensearch.cluster.metadata.MetadataIndexTemplateService.findComponentTemplate;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.opensearch.index.IndexSettings.same;

/**
 * Service responsible for submitting update index settings requests
 *
 * @opensearch.internal
 */
public class MetadataUpdateSettingsService {
    private static final Logger logger = LogManager.getLogger(MetadataUpdateSettingsService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ShardLimitValidator shardLimitValidator;
    private final ThreadPool threadPool;
    private final ClusterManagerTaskThrottler.ThrottlingKey updateSettingsTaskKey;

    private AwarenessReplicaBalance awarenessReplicaBalance;

    @Inject
    public MetadataUpdateSettingsService(
        ClusterService clusterService,
        AllocationService allocationService,
        IndexScopedSettings indexScopedSettings,
        IndicesService indicesService,
        ShardLimitValidator shardLimitValidator,
        ThreadPool threadPool,
        AwarenessReplicaBalance awarenessReplicaBalance
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
        this.shardLimitValidator = shardLimitValidator;
        this.awarenessReplicaBalance = awarenessReplicaBalance;

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        updateSettingsTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTaskKeys.UPDATE_SETTINGS_KEY, true);
    }

    public void updateSettings(
        final UpdateSettingsClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        final Settings normalizedSettings = Settings.builder()
            .put(request.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();

        validateRefreshIntervalSettings(normalizedSettings, clusterService.getClusterSettings());
        validateTranslogDurabilitySettings(normalizedSettings, clusterService.getClusterSettings(), clusterService.getSettings());
        validateIndexTotalPrimaryShardsPerNodeSetting(normalizedSettings, clusterService);
        final int defaultReplicaCount = clusterService.getClusterSettings().get(Metadata.DEFAULT_REPLICA_COUNT_SETTING);

        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        indexScopedSettings.validate(
            normalizedSettings.filter(s -> Regex.isSimpleMatchPattern(s) == false), // don't validate wildcards
            false, // don't validate dependencies here we check it below never allow to change the number of shards
            false,
            true, // Ignore archived setting.
            true
        ); // validate internal or private index settings
        for (String key : normalizedSettings.keySet()) {
            Setting setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            boolean isArchived = key.startsWith(ARCHIVED_SETTINGS_PREFIX);
            assert setting != null // we already validated the normalized settings
                || isArchived
                || (isWildcard && normalizedSettings.hasValue(key) == false) : "unknown setting: "
                    + key
                    + " isWildcard: "
                    + isWildcard
                    + " hasValue: "
                    + normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            // Only allow dynamic settings and wildcards for open indices. Skip archived settings.
            if (isArchived == false && (isWildcard || setting.isDynamic())) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key);
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        final boolean preserveExisting = request.isPreserveExisting();

        clusterService.submitStateUpdateTask(
            "update-settings " + Arrays.toString(request.indices()),
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(
                Priority.URGENT,
                request,
                wrapPreservingContext(listener, threadPool.getThreadContext())
            ) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return updateSettingsTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {

                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                    // allow to change any settings to a close index, and only allow dynamic settings to be changed
                    // on an open index
                    Set<Index> openIndices = new HashSet<>();
                    Set<Index> closeIndices = new HashSet<>();
                    final String[] actualIndices = new String[request.indices().length];
                    final List<String> validationErrors = new ArrayList<>();
                    for (int i = 0; i < request.indices().length; i++) {
                        Index index = request.indices()[i];
                        actualIndices[i] = index.getName();
                        final IndexMetadata metadata = currentState.metadata().getIndexSafe(index);
                        if (metadata.getState() == IndexMetadata.State.OPEN) {
                            openIndices.add(index);
                        } else {
                            closeIndices.add(index);
                        }
                        if (metadata.context() != null) {
                            validateOverlap(
                                normalizedSettings.keySet(),
                                findComponentTemplate(currentState.metadata(), metadata.context()).template().settings(),
                                index.getName()
                            ).ifPresent(validationErrors::add);
                        }
                        validateTranslogFlushIntervalSettingsForCompositeIndex(
                            normalizedSettings,
                            clusterService.getClusterSettings(),
                            metadata.getSettings()
                        ).ifPresent(validationErrors::add);

                    }

                    if (validationErrors.size() > 0) {
                        ValidationException exception = new ValidationException();
                        exception.addValidationErrors(validationErrors);
                        throw exception;
                    }

                    if (!skippedSettings.isEmpty() && !openIndices.isEmpty()) {
                        throw new IllegalArgumentException(
                            String.format(
                                Locale.ROOT,
                                "Can't update non dynamic settings [%s] for open indices %s",
                                skippedSettings,
                                openIndices
                            )
                        );
                    }

                    if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(openSettings)) {
                        final int updatedNumberOfReplicas = openSettings.getAsInt(
                            IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                            defaultReplicaCount
                        );
                        if (preserveExisting == false) {
                            for (Index index : request.indices()) {
                                if (index.getName().charAt(0) != '.') {
                                    // No replica count validation for system indices
                                    Optional<String> error = awarenessReplicaBalance.validate(
                                        updatedNumberOfReplicas,
                                        AutoExpandReplicas.SETTING.get(openSettings)
                                    );

                                    if (error.isPresent()) {
                                        ValidationException ex = new ValidationException();
                                        ex.addValidationError(error.get());
                                        throw ex;
                                    }
                                }
                            }

                            // Verify that this won't take us over the cluster shard limit.
                            int totalNewShards = Arrays.stream(request.indices())
                                .mapToInt(i -> getTotalNewShards(i, currentState, updatedNumberOfReplicas))
                                .sum();
                            Optional<String> error = shardLimitValidator.checkShardLimit(totalNewShards, currentState);
                            if (error.isPresent()) {
                                ValidationException ex = new ValidationException();
                                ex.addValidationError(error.get());
                                throw ex;
                            }

                            /*
                             * We do not update the in-sync allocation IDs as they will be removed upon the first index operation which makes
                             * these copies stale.
                             *
                             * TODO: should we update the in-sync allocation IDs once the data is deleted by the node?
                             */
                            routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                            metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                            logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                        }
                    }

                    if (IndexMetadata.INDEX_NUMBER_OF_SEARCH_REPLICAS_SETTING.exists(openSettings)) {
                        validateSearchReplicaCountSettings(normalizedSettings, request.indices(), currentState);
                        final int updatedNumberOfSearchReplicas = IndexMetadata.INDEX_NUMBER_OF_SEARCH_REPLICAS_SETTING.get(openSettings);
                        if (preserveExisting == false) {
                            for (Index index : request.indices()) {
                                if (index.getName().charAt(0) != '.') {
                                    // No replica count validation for system indices
                                    Optional<String> error = awarenessReplicaBalance.validate(
                                        updatedNumberOfSearchReplicas,
                                        AutoExpandSearchReplicas.SETTING.get(openSettings)
                                    );

                                    if (error.isPresent()) {
                                        ValidationException ex = new ValidationException();
                                        ex.addValidationError(error.get());
                                        throw ex;
                                    }
                                }
                            }

                            // Verify that this won't take us over the cluster shard limit.
                            int totalNewShards = Arrays.stream(request.indices())
                                .mapToInt(i -> getTotalNewShards(i, currentState, updatedNumberOfSearchReplicas))
                                .sum();
                            Optional<String> error = shardLimitValidator.checkShardLimit(totalNewShards, currentState);
                            if (error.isPresent()) {
                                ValidationException ex = new ValidationException();
                                ex.addValidationError(error.get());
                                throw ex;
                            }
                            routingTableBuilder.updateNumberOfSearchReplicas(updatedNumberOfSearchReplicas, actualIndices);
                            metadataBuilder.updateNumberOfSearchReplicas(updatedNumberOfSearchReplicas, actualIndices);
                            logger.info(
                                "updating number_of_Search Replicas to [{}] for indices {}",
                                updatedNumberOfSearchReplicas,
                                actualIndices
                            );
                        }
                    }

                    if (!openIndices.isEmpty()) {
                        for (Index index : openIndices) {
                            IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                            Settings.Builder updates = Settings.builder();
                            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                            if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                                if (preserveExisting) {
                                    indexSettings.put(indexMetadata.getSettings());
                                }
                                /*
                                 * The setting index.number_of_replicas is special; we require that this setting has a value in the index. When
                                 * creating the index, we ensure this by explicitly providing a value for the setting to the default (one) if
                                 * there is no value provided on the source of the index creation or "cluster.default_number_of_replicas" is
                                 * not set. A user can update this setting though, including updating it to null, indicating that they want to
                                 * use the value configured by cluster settings or a default value 1. In this case, we again have to provide
                                 * an explicit value for the setting.
                                 */
                                if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(indexSettings) == false) {
                                    indexSettings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, defaultReplicaCount);
                                }
                                Settings finalSettings = indexSettings.build();
                                indexScopedSettings.validate(
                                    finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false),
                                    true
                                );
                                metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                            }
                        }
                    }

                    if (!closeIndices.isEmpty()) {
                        for (Index index : closeIndices) {
                            IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                            Settings.Builder updates = Settings.builder();
                            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                            if (indexScopedSettings.updateSettings(closedSettings, indexSettings, updates, index.getName())) {
                                if (preserveExisting) {
                                    indexSettings.put(indexMetadata.getSettings());
                                }
                                /*
                                 * The setting index.number_of_replicas is special; we require that this setting has a value in the index. When
                                 * creating the index, we ensure this by explicitly providing a value for the setting to the default (one) if
                                 * there is a not value provided on the source of the index creation. A user can update this setting though,
                                 * including updating it to null, indicating that they want to use the default value. In this case, we again
                                 * have to provide an explicit value for the setting to the default (one).
                                 */
                                if (IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(indexSettings) == false) {
                                    indexSettings.put(
                                        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                                        IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(Settings.EMPTY)
                                    );
                                }
                                Settings finalSettings = indexSettings.build();
                                indexScopedSettings.validate(
                                    finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false),
                                    true,
                                    false,
                                    true
                                );
                                metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                            }
                        }
                    }

                    if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(normalizedSettings)
                        || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(normalizedSettings)) {
                        Settings indexSettings;
                        for (String index : actualIndices) {
                            indexSettings = metadataBuilder.get(index).getSettings();
                            MetadataCreateIndexService.validateTranslogRetentionSettings(indexSettings);
                            // validate storeType for deprecating index stores
                            MetadataCreateIndexService.validateStoreTypeSettings(indexSettings);
                        }
                    }
                    boolean changed = false;
                    // increment settings versions
                    for (final String index : actualIndices) {
                        if (same(currentState.metadata().index(index).getSettings(), metadataBuilder.get(index).getSettings()) == false) {
                            changed = true;
                            final IndexMetadata.Builder builder = IndexMetadata.builder(metadataBuilder.get(index));
                            builder.settingsVersion(1 + builder.settingsVersion());
                            metadataBuilder.put(builder);
                        }
                    }

                    final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    for (IndexMetadata.APIBlock block : IndexMetadata.APIBlock.values()) {
                        changed |= maybeUpdateClusterBlock(actualIndices, blocks, block.block, block.setting, openSettings);
                    }

                    if (changed == false) {
                        return currentState;
                    }

                    ClusterState updatedState = ClusterState.builder(currentState)
                        .metadata(metadataBuilder)
                        .routingTable(routingTableBuilder.build())
                        .blocks(blocks)
                        .build();
                    // now, reroute in case things change that require it (like number of replicas)
                    updatedState = allocationService.reroute(updatedState, "settings update");
                    try {
                        for (Index index : openIndices) {
                            final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                            final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                            indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                        }
                        for (Index index : closeIndices) {
                            final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                            final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                            // Verifies that the current index settings can be updated with the updated dynamic settings.
                            indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                            // Now check that we can create the index with the updated settings (dynamic and non-dynamic).
                            // This step is mandatory since we allow to update non-dynamic settings on closed indices.
                            indicesService.verifyIndexMetadata(updatedMetadata, updatedMetadata);
                        }
                    } catch (IOException ex) {
                        throw ExceptionsHelper.convertToOpenSearchException(ex);
                    }
                    return updatedState;
                }
            }
        );
    }

    private int getTotalNewShards(Index index, ClusterState currentState, int updatedNumberOfReplicas) {
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        int shardsInIndex = indexMetadata.getNumberOfShards();
        int oldNumberOfReplicas = indexMetadata.getNumberOfReplicas();
        int replicaIncrease = updatedNumberOfReplicas - oldNumberOfReplicas;
        return replicaIncrease * shardsInIndex;
    }

    /**
     * Updates the cluster block only iff the setting exists in the given settings
     */
    private static boolean maybeUpdateClusterBlock(
        String[] actualIndices,
        ClusterBlocks.Builder blocks,
        ClusterBlock block,
        Setting<Boolean> setting,
        Settings openSettings
    ) {
        boolean changed = false;
        if (setting.exists(openSettings)) {
            final boolean updateBlock = setting.get(openSettings);
            for (String index : actualIndices) {
                if (updateBlock) {
                    if (blocks.hasIndexBlock(index, block) == false) {
                        blocks.addIndexBlock(index, block);
                        changed = true;
                    }
                } else {
                    if (blocks.hasIndexBlock(index, block)) {
                        blocks.removeIndexBlock(index, block);
                        changed = true;
                    }
                }
            }
        }
        return changed;
    }

    public void upgradeIndexSettings(
        final UpgradeSettingsClusterStateUpdateRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "update-index-compatibility-versions",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(
                Priority.URGENT,
                request,
                wrapPreservingContext(listener, threadPool.getThreadContext())
            ) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (Map.Entry<String, Tuple<Version, String>> entry : request.versions().entrySet()) {
                        String index = entry.getKey();
                        IndexMetadata indexMetadata = metadataBuilder.get(index);
                        if (indexMetadata != null) {
                            if (Version.CURRENT.equals(indexMetadata.getCreationVersion()) == false) {
                                // no reason to pollute the settings, we didn't really upgrade anything
                                metadataBuilder.put(
                                    IndexMetadata.builder(indexMetadata)
                                        .settings(
                                            Settings.builder()
                                                .put(indexMetadata.getSettings())
                                                .put(IndexMetadata.SETTING_VERSION_UPGRADED, entry.getValue().v1())
                                        )
                                        .settingsVersion(1 + indexMetadata.getSettingsVersion())
                                );
                            }
                        }
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }
            }
        );
    }

    /**
     * Validates that if we are trying to update search replica count the index is segrep enabled.
     *
     * @param requestSettings {@link Settings}
     * @param indices indices that are changing
     * @param currentState {@link ClusterState} current cluster state
     */
    private void validateSearchReplicaCountSettings(Settings requestSettings, Index[] indices, ClusterState currentState) {
        final int updatedNumberOfSearchReplicas = IndexMetadata.INDEX_NUMBER_OF_SEARCH_REPLICAS_SETTING.get(requestSettings);
        if (updatedNumberOfSearchReplicas > 0) {
            if (Arrays.stream(indices)
                .allMatch(
                    index -> currentState.metadata().index(index.getName()).getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false)
                ) == false) {
                throw new IllegalArgumentException(
                    "To set " + SETTING_NUMBER_OF_SEARCH_REPLICAS + ", " + SETTING_REMOTE_STORE_ENABLED + " must be set to true"
                );
            }
        }
    }

    /**
     * Validates the 'index.routing.allocation.total_primary_shards_per_node' setting during index settings update.
     * Ensures this setting can only be modified for existing indices in remote store enabled clusters.
     */
    public static void validateIndexTotalPrimaryShardsPerNodeSetting(Settings indexSettings, ClusterService clusterService) {
        // Get the setting value
        int indexPrimaryShardsPerNode = INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.get(indexSettings);

        // If default value (-1), no validation needed
        if (indexPrimaryShardsPerNode == -1) {
            return;
        }

        // Check if remote store is enabled
        boolean isRemoteStoreEnabled = clusterService.state()
            .nodes()
            .getNodes()
            .values()
            .stream()
            .allMatch(DiscoveryNode::isRemoteStoreNode);
        if (!isRemoteStoreEnabled) {
            throw new IllegalArgumentException(
                "Setting [" + INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey() + "] can only be used with remote store enabled clusters"
            );
        }
    }
}
