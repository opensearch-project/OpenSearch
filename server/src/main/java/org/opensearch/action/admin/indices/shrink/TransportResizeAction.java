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

package org.opensearch.action.admin.indices.shrink;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;
import org.opensearch.node.remotestore.RemoteStoreNodeService.Direction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;

/**
 * Main class to initiate resizing (shrink / split) an index into a new index
 *
 * @opensearch.internal
 */
public class TransportResizeAction extends TransportClusterManagerNodeAction<ResizeRequest, ResizeResponse> {
    private final MetadataCreateIndexService createIndexService;
    private final Client client;

    @Inject
    public TransportResizeAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        this(
            ResizeAction.NAME,
            transportService,
            clusterService,
            threadPool,
            createIndexService,
            actionFilters,
            indexNameExpressionResolver,
            client
        );
    }

    protected TransportResizeAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, ResizeRequest::new, indexNameExpressionResolver);
        this.createIndexService = createIndexService;
        this.client = client;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ResizeResponse read(StreamInput in) throws IOException {
        return new ResizeResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ResizeRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getTargetIndexRequest().index());
    }

    @Override
    protected void clusterManagerOperation(
        final ResizeRequest resizeRequest,
        final ClusterState state,
        final ActionListener<ResizeResponse> listener
    ) {

        // there is no need to fetch docs stats for split but we keep it simple and do it anyway for simplicity of the code
        final String sourceIndex = indexNameExpressionResolver.resolveDateMathExpression(resizeRequest.getSourceIndex());
        final String targetIndex = indexNameExpressionResolver.resolveDateMathExpression(resizeRequest.getTargetIndexRequest().index());
        IndexMetadata indexMetadata = state.metadata().index(sourceIndex);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        if (resizeRequest.getResizeType().equals(ResizeType.SHRINK)
            && state.metadata().isSegmentReplicationEnabled(sourceIndex)
            && indexMetadata != null
            && Integer.valueOf(indexMetadata.getSettings().get(SETTING_NUMBER_OF_REPLICAS)) > 0) {
            client.admin()
                .indices()
                .prepareRefresh(sourceIndex)
                .execute(ActionListener.delegateFailure(listener, (delegatedRefreshListener, refreshResponse) -> {
                    client.admin()
                        .indices()
                        .prepareStats(sourceIndex)
                        .clear()
                        .setDocs(true)
                        .setStore(true)
                        .setSegments(true)
                        .execute(ActionListener.delegateFailure(listener, (delegatedIndicesStatsListener, indicesStatsResponse) -> {
                            CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(resizeRequest, state, i -> {
                                IndexShardStats shard = indicesStatsResponse.getIndex(sourceIndex).getIndexShards().get(i);
                                return shard == null ? null : shard.getPrimary().getDocs();
                            }, indicesStatsResponse.getPrimaries().store, clusterSettings, sourceIndex, targetIndex);

                            if (indicesStatsResponse.getIndex(sourceIndex)
                                .getTotal()
                                .getSegments()
                                .getReplicationStats().maxBytesBehind != 0) {
                                throw new IllegalStateException(
                                    "Replication still in progress for index ["
                                        + sourceIndex
                                        + "]. Please wait for replication to complete and retry. Use the _cat/segment_replication/"
                                        + sourceIndex
                                        + " api to check if the index is up to date (e.g. bytes_behind == 0)."
                                );
                            }

                            createIndexService.createIndex(
                                updateRequest,
                                ActionListener.map(
                                    delegatedIndicesStatsListener,
                                    response -> new ResizeResponse(
                                        response.isAcknowledged(),
                                        response.isShardsAcknowledged(),
                                        updateRequest.index()
                                    )
                                )
                            );
                        }));
                }));
        } else {
            client.admin()
                .indices()
                .prepareStats(sourceIndex)
                .clear()
                .setDocs(true)
                .setStore(true)
                .execute(ActionListener.delegateFailure(listener, (delegatedListener, indicesStatsResponse) -> {
                    CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(resizeRequest, state, i -> {
                        IndexShardStats shard = indicesStatsResponse.getIndex(sourceIndex).getIndexShards().get(i);
                        return shard == null ? null : shard.getPrimary().getDocs();
                    }, indicesStatsResponse.getPrimaries().store, clusterSettings, sourceIndex, targetIndex);
                    createIndexService.createIndex(
                        updateRequest,
                        ActionListener.map(
                            delegatedListener,
                            response -> new ResizeResponse(
                                response.isAcknowledged(),
                                response.isShardsAcknowledged(),
                                updateRequest.index()
                            )
                        )
                    );
                }));
        }

    }

    // static for unittesting this method
    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(
        final ResizeRequest resizeRequest,
        final ClusterState state,
        final IntFunction<DocsStats> perShardDocStats,
        final StoreStats primaryShardsStoreStats,
        final ClusterSettings clusterSettings,
        String sourceIndexName,
        String targetIndexName
    ) {
        final CreateIndexRequest targetIndex = resizeRequest.getTargetIndexRequest();
        final IndexMetadata metadata = state.metadata().index(sourceIndexName);
        if (metadata == null) {
            throw new IndexNotFoundException(sourceIndexName);
        }
        validateRemoteMigrationModeSettings(resizeRequest.getResizeType(), metadata, clusterSettings);
        final Settings.Builder targetIndexSettingsBuilder = Settings.builder()
            .put(targetIndex.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        targetIndexSettingsBuilder.remove(IndexMetadata.SETTING_HISTORY_UUID);
        final Settings targetIndexSettings = targetIndexSettingsBuilder.build();
        final int numShards;

        // We should check the source index's setting `index.blocks.read_only`, because the setting will be copied to target index,
        // it will block target index's metadata writes and then cause the new shards to be unassigned,
        // but if user overwrites the setting to `false` or `null`, everything is fine.
        // We don't need to check the setting `index.blocks.metadata`, because it was checked when fetching index stats
        if (IndexMetadata.INDEX_READ_ONLY_SETTING.get(metadata.getSettings()) == true
            && IndexMetadata.INDEX_READ_ONLY_SETTING.exists(targetIndexSettings) == false) {
            throw new IllegalArgumentException(
                "target index ["
                    + targetIndexName
                    + "] will be blocked by [index.blocks.read_only=true] which is copied from the source index ["
                    + sourceIndexName
                    + "], this will disable metadata writes and cause the shards to be unassigned"
            );
        }

        if (IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            numShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings);
        } else {
            assert resizeRequest.getResizeType() != ResizeType.SPLIT : "split must specify the number of shards explicitly";
            if (resizeRequest.getResizeType() == ResizeType.SHRINK) {
                numShards = calculateTargetIndexShardsNum(resizeRequest.getMaxShardSize(), primaryShardsStoreStats, metadata);
            } else {
                assert resizeRequest.getResizeType() == ResizeType.CLONE;
                numShards = metadata.getNumberOfShards();
            }
        }

        for (int i = 0; i < numShards; i++) {
            if (resizeRequest.getResizeType() == ResizeType.SHRINK) {
                Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(i, metadata, numShards);
                long count = 0;
                for (ShardId id : shardIds) {
                    DocsStats docsStats = perShardDocStats.apply(id.id());
                    if (docsStats != null) {
                        count += docsStats.getCount();
                    }
                    if (count > IndexWriter.MAX_DOCS) {
                        throw new IllegalStateException(
                            "Can't merge index with more than ["
                                + IndexWriter.MAX_DOCS
                                + "] docs - too many documents in shards "
                                + shardIds
                        );
                    }
                }
            } else if (resizeRequest.getResizeType() == ResizeType.SPLIT) {
                Objects.requireNonNull(IndexMetadata.selectSplitShard(i, metadata, numShards));
                // we just execute this to ensure we get the right exceptions if the number of shards is wrong or less then etc.
            } else {
                Objects.requireNonNull(IndexMetadata.selectCloneShard(i, metadata, numShards));
                // we just execute this to ensure we get the right exceptions if the number of shards is wrong etc.
            }
        }

        if (IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(targetIndexSettings)) {
            // if we have a source index with 1 shards it's legal to set this
            final boolean splitFromSingleShards = resizeRequest.getResizeType() == ResizeType.SPLIT && metadata.getNumberOfShards() == 1;
            if (splitFromSingleShards == false) {
                throw new IllegalArgumentException("cannot provide index.number_of_routing_shards on resize");
            }
        }
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(metadata.getSettings())
            && IndexSettings.INDEX_SOFT_DELETES_SETTING.exists(targetIndexSettings)
            && IndexSettings.INDEX_SOFT_DELETES_SETTING.get(targetIndexSettings) == false) {
            throw new IllegalArgumentException("Can't disable [index.soft_deletes.enabled] setting on resize");
        }
        if (IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.exists(targetIndexSettings)) {
            throw new IllegalArgumentException("cannot provide [index.replication.type] setting on resize");
        }
        String cause = resizeRequest.getResizeType().name().toLowerCase(Locale.ROOT) + "_index";
        targetIndex.cause(cause);
        Settings.Builder settingsBuilder = Settings.builder().put(targetIndexSettings);
        settingsBuilder.put("index.number_of_shards", numShards);
        targetIndex.settings(settingsBuilder);

        return new CreateIndexClusterStateUpdateRequest(cause, targetIndex.index(), targetIndexName)
            // mappings are updated on the node when creating in the shards, this prevents race-conditions since all mapping must be
            // applied once we took the snapshot and if somebody messes things up and switches the index read/write and adds docs we
            // miss the mappings for everything is corrupted and hard to debug
            .ackTimeout(targetIndex.timeout())
            .masterNodeTimeout(targetIndex.clusterManagerNodeTimeout())
            .settings(targetIndex.settings())
            .aliases(targetIndex.aliases())
            .waitForActiveShards(targetIndex.waitForActiveShards())
            .recoverFrom(metadata.getIndex())
            .resizeType(resizeRequest.getResizeType())
            .copySettings(resizeRequest.getCopySettings() == null ? false : resizeRequest.getCopySettings());
    }

    /**
     * Calculate target index's shards count according to max_shard_ize and the source index's storage(only primary shards included)
     * for shrink. Target index's shards count is the lowest factor of the source index's primary shards count which satisfies the
     * maximum shard size requirement. If max_shard_size is less than the source index's single shard size, then target index's shards count
     * will be equal to the source index's shards count.
     * @param maxShardSize the maximum size of a primary shard in the target index
     * @param sourceIndexShardStoreStats primary shards' store stats of the source index
     * @param sourceIndexMetaData source index's metadata
     * @return target index's shards number
     */
    protected static int calculateTargetIndexShardsNum(
        ByteSizeValue maxShardSize,
        StoreStats sourceIndexShardStoreStats,
        IndexMetadata sourceIndexMetaData
    ) {
        if (maxShardSize == null
            || sourceIndexShardStoreStats == null
            || maxShardSize.getBytes() == 0
            || sourceIndexShardStoreStats.getSizeInBytes() == 0) {
            return 1;
        }

        int sourceIndexShardsNum = sourceIndexMetaData.getNumberOfShards();
        // calculate the minimum shards count according to source index's storage, ceiling ensures that the minimum shards count is never
        // less than 1
        int minValue = (int) Math.ceil((double) sourceIndexShardStoreStats.getSizeInBytes() / maxShardSize.getBytes());
        // if minimum shards count is greater than the source index's shards count, then the source index's shards count will be returned
        if (minValue >= sourceIndexShardsNum) {
            return sourceIndexShardsNum;
        }

        // find the lowest factor of the source index's shards count here, because minimum shards count may not be a factor
        for (int i = minValue; i < sourceIndexShardsNum; i++) {
            if (sourceIndexShardsNum % i == 0) {
                return i;
            }
        }
        return sourceIndexShardsNum;
    }

    @Override
    protected String getClusterManagerActionName(DiscoveryNode node) {
        return super.getClusterManagerActionName(node);
    }

    /**
     * Reject resize request if cluster mode is [Mixed] and migration direction is [RemoteStore] and index is not on
     * REMOTE_STORE_ENABLED node or [DocRep] and index is on REMOTE_STORE_ENABLED node.
     * @param type resize type
     * @param sourceIndexMetadata source index's metadata
     * @param clusterSettings cluster settings
     * @throws IllegalStateException if cluster mode is [Mixed] and migration direction is [RemoteStore] or [DocRep] and
     *                               index's SETTING_REMOTE_STORE_ENABLED is not equal to the migration direction's value.
     *                               For example, if migration direction is [RemoteStore] and index's SETTING_REMOTE_STORE_ENABLED
     *                               is false, then throw IllegalStateException. If migration direction is [DocRep] and
     *                               index's SETTING_REMOTE_STORE_ENABLED is true, then throw IllegalStateException.
     */
    private static void validateRemoteMigrationModeSettings(
        final ResizeType type,
        IndexMetadata sourceIndexMetadata,
        ClusterSettings clusterSettings
    ) {
        CompatibilityMode compatibilityMode = clusterSettings.get(RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING);
        if (compatibilityMode == CompatibilityMode.MIXED) {
            boolean isRemoteStoreEnabled = sourceIndexMetadata.getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false);
            Direction migrationDirection = clusterSettings.get(RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING);
            boolean invalidConfiguration = (migrationDirection == Direction.REMOTE_STORE && isRemoteStoreEnabled == false)
                || (migrationDirection == Direction.DOCREP && isRemoteStoreEnabled);
            if (invalidConfiguration) {
                throw new IllegalStateException(
                    "Index "
                        + type
                        + " is not allowed as remote migration mode is mixed"
                        + " and index is remote store "
                        + (isRemoteStoreEnabled ? "enabled" : "disabled")
                );
            }
        }
    }
}
