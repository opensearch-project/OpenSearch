/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.PrimaryMissingActionException;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.AckedClusterStateTaskListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.CompositeIndexValidator;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.core.xcontent.MediaTypeRegistry.JSON;

/**
 * Transport action for the star tree upgrade operation.
 * Extends {@link TransportBroadcastByNodeAction} to perform a two-phase upgrade:
 * <ol>
 *   <li>Phase 1: Submit a mapping update to add the star tree field configuration to the index mapping,
 *       bypassing the {@code index.composite_index} setting check via {@link MergeReason#STAR_TREE_UPGRADE}.</li>
 *   <li>Phase 2: Per-shard star tree building and SegmentInfos rewrite via the broadcast mechanism, which calls
 *       {@link IndexShard#upgradeToStarTree(StarTreeField)} on each shard.</li>
 * </ol>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportStarTreeUpgradeAction extends TransportBroadcastByNodeAction<
    StarTreeUpgradeRequest,
    StarTreeUpgradeResponse,
    ShardStarTreeUpgradeResult> {

    private static final Logger logger = LogManager.getLogger(TransportStarTreeUpgradeAction.class);

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final StarTreeUpgradeMappingExecutor mappingExecutor = new StarTreeUpgradeMappingExecutor();

    @Inject
    public TransportStarTreeUpgradeAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            StarTreeUpgradeAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            StarTreeUpgradeRequest::new,
            ThreadPool.Names.FORCE_MERGE
        );
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, StarTreeUpgradeRequest request, ActionListener<StarTreeUpgradeResponse> listener) {
        // Resolve indices and validate before submitting mapping update
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);

        // Check for existing star tree config (idempotency)
        boolean allIndicesHaveStarTree = true;
        for (String indexName : concreteIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
            if (indexMetadata == null) {
                listener.onFailure(new IllegalArgumentException("index [" + indexName + "] not found"));
                return;
            }
            if (hasExistingStarTreeConfig(indexMetadata) == false) {
                allIndicesHaveStarTree = false;
            }
        }

        if (allIndicesHaveStarTree) {
            // Star tree already in mapping for all indices - skip mapping update, go straight to broadcast
            // for engine restart + force merge (handles partial retry case)
            super.doExecute(task, request, listener);
            return;
        }

        // Validate that no index already has a different star tree config
        for (String indexName : concreteIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
            if (hasExistingStarTreeConfig(indexMetadata)) {
                listener.onFailure(
                    new IllegalArgumentException("index [" + indexName + "] already has star tree configuration in its mapping")
                );
                return;
            }
        }

        // Phase 1: Submit mapping update to cluster state
        submitMappingUpdate(request, concreteIndices, ActionListener.wrap(ack -> {
            if (ack) {
                // Phase 2 + 3: Engine restart + force merge (per shard, via broadcast)
                super.doExecute(task, request, listener);
            } else {
                listener.onFailure(new IOException("mapping update was not acknowledged by all nodes"));
            }
        }, listener::onFailure));
    }

    @Override
    protected ShardStarTreeUpgradeResult shardOperation(StarTreeUpgradeRequest request, ShardRouting shardRouting) throws IOException {
        logger.info("[CODEC_CHECK] shardOperation START for {}", shardRouting.shardId());
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());

        // Verify mapping is available on this node before proceeding
        if (indexShard.mapperService().getCompositeFieldTypes().isEmpty()) {
            throw new IOException(
                "Star tree field not yet available in MapperService on shard ["
                    + shardRouting.shardId()
                    + "]. Cluster state may not have propagated yet. Retry."
            );
        }

        try {
            indexShard.upgradeToStarTree(request.getStarTreeField());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("star tree upgrade interrupted on shard [" + shardRouting.shardId() + "]", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IOException("star tree upgrade timed out on shard [" + shardRouting.shardId() + "]", e);
        }

        // Post-upgrade flush to clear pending translog recovery state from engine swap.
        indexShard.flush(new FlushRequest().force(false).waitIfOngoing(true));

        // [CODEC_CHECK] Log the codec of each segment after upgrade + flush
        try {
            org.apache.lucene.index.SegmentInfos infos = org.apache.lucene.index.SegmentInfos.readLatestCommit(
                indexShard.store().directory()
            );
            for (org.apache.lucene.index.SegmentCommitInfo ci : infos) {
                logger.info("[CODEC_CHECK] segment={} codec={} maxDoc={}", ci.info.name, ci.info.getCodec().getName(), ci.info.maxDoc());
            }
        } catch (Exception e) {
            logger.warn("[CODEC_CHECK] ERROR reading segment infos", e);
        }

        return new ShardStarTreeUpgradeResult(shardRouting.shardId(), shardRouting.primary());
    }

    @Override
    protected StarTreeUpgradeResponse newResponse(
        StarTreeUpgradeRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardStarTreeUpgradeResult> shardResults,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new StarTreeUpgradeResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardStarTreeUpgradeResult readShardResult(StreamInput in) throws IOException {
        return new ShardStarTreeUpgradeResult(in);
    }

    @Override
    protected StarTreeUpgradeRequest readRequestFrom(StreamInput in) throws IOException {
        return new StarTreeUpgradeRequest(in);
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, StarTreeUpgradeRequest request, String[] concreteIndices) {
        ShardsIterator iterator = clusterState.routingTable().allShards(concreteIndices);
        Set<String> indicesWithMissingPrimaries = indicesWithMissingPrimaries(clusterState, concreteIndices);
        if (indicesWithMissingPrimaries.isEmpty()) {
            return iterator;
        }
        throw new PrimaryMissingActionException(
            "Cannot upgrade indices because the following indices are missing primary shards " + indicesWithMissingPrimaries
        );
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, StarTreeUpgradeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, StarTreeUpgradeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }

    /**
     * Checks whether the given index metadata already has a star tree (composite) field configured.
     */
    private static boolean hasExistingStarTreeConfig(IndexMetadata indexMetadata) {
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            return false;
        }
        Map<String, Object> sourceMap = mappingMetadata.sourceAsMap();
        Object composite = sourceMap.get("composite");
        return composite instanceof Map && ((Map<?, ?>) composite).isEmpty() == false;
    }

    /**
     * Finds all indices that do not have all primaries available.
     */
    private Set<String> indicesWithMissingPrimaries(ClusterState clusterState, String[] concreteIndices) {
        Set<String> indices = new HashSet<>();
        RoutingTable routingTable = clusterState.routingTable();
        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = routingTable.index(index);
            if (indexRoutingTable.allPrimaryShardsActive() == false) {
                indices.add(index);
            }
        }
        return indices;
    }

    // ---- Mapping Update Logic ----

    /**
     * Submits a cluster state update that adds the star tree field to the index mapping.
     * Uses a custom {@link ClusterStateTaskExecutor} that:
     * <ol>
     *   <li>Creates a {@link MapperService} for the target index</li>
     *   <li>Sets the {@code allowCompositeFieldWithoutSettings} flag on the parser context</li>
     *   <li>Merges the star tree mapping with {@link MergeReason#STAR_TREE_UPGRADE}</li>
     *   <li>Calls {@link CompositeIndexValidator#validate} with the upgrade merge reason</li>
     *   <li>Commits the updated mapping to cluster state</li>
     * </ol>
     */
    private void submitMappingUpdate(StarTreeUpgradeRequest request, String[] concreteIndices, ActionListener<Boolean> listener) {
        StarTreeUpgradeMappingTask mappingTask = new StarTreeUpgradeMappingTask(request, concreteIndices);
        clusterService.submitStateUpdateTask(
            "star-tree-upgrade-mapping",
            mappingTask,
            ClusterStateTaskConfig.build(Priority.HIGH),
            mappingExecutor,
            new AckedClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    listener.onResponse(e == null);
                }

                @Override
                public void onAckTimeout() {
                    listener.onResponse(false);
                }

                @Override
                public TimeValue ackTimeout() {
                    return TimeValue.timeValueMinutes(1);
                }
            }
        );
    }

    /**
     * Builds a complete mapping source that includes the existing properties from the index
     * alongside the new composite (star tree) section. This is necessary because
     * {@code StarTreeMapper.Builder} validates dimension/metric fields by looking them up
     * in the {@code ObjectMapper.Builder}'s mapper builders list, which only contains
     * fields from the current mapping source being parsed.
     */
    static CompressedXContent buildCompleteMappingSource(StarTreeUpgradeRequest request, IndexMetadata indexMetadata) throws IOException {
        // Get existing mapping source
        MappingMetadata existingMapping = indexMetadata.mapping();
        Map<String, Object> existingSourceMap = new HashMap<>();
        if (existingMapping != null) {
            Map<String, Object> fullSource = existingMapping.sourceAsMap();
            // Copy properties and other top-level mapping fields
            for (Map.Entry<String, Object> entry : fullSource.entrySet()) {
                if ("composite".equals(entry.getKey()) == false) {
                    existingSourceMap.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // Build the composite section with the star tree field
        Map<String, Object> compositeSection = new HashMap<>();
        Map<String, Object> starTreeFieldMap = new HashMap<>();
        starTreeFieldMap.put("type", "star_tree");

        // Serialize StarTreeField config — use jsonBuilder directly since toXContent() already
        // writes startObject/endObject.
        BytesReference configBytes;
        try (XContentBuilder configBuilder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            request.getStarTreeField().toXContent(configBuilder, ToXContent.EMPTY_PARAMS);
            configBytes = BytesReference.bytes(configBuilder);
        }
        Map<String, Object> configMap = XContentHelper.convertToMap(configBytes, false, JSON).v2();

        // Remove fields from configMap that StarTreeMapper.Builder doesn't expect in the config.
        configMap.remove("name");
        Object orderedDims = configMap.get("ordered_dimensions");
        if (orderedDims instanceof List) {
            for (Object dim : (List<?>) orderedDims) {
                if (dim instanceof Map) {
                    ((Map<?, ?>) dim).remove("type");
                }
            }
        }

        starTreeFieldMap.put("config", configMap);

        compositeSection.put(request.getStarTreeField().getName(), starTreeFieldMap);
        existingSourceMap.put("composite", compositeSection);

        // Wrap in _doc type
        Map<String, Object> fullMapping = new HashMap<>();
        fullMapping.put(MapperService.SINGLE_MAPPING_NAME, existingSourceMap);

        XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
        builder.map(fullMapping);
        builder.close();
        return new CompressedXContent(builder.getOutputStream().toString());
    }

    /**
     * Builds the mapping source JSON that includes the star tree field under the "composite" section.
     * The resulting mapping looks like:
     * <pre>
     * {
     *   "_doc": {
     *     "composite": {
     *       "my_star_tree": {
     *         "type": "star_tree",
     *         "config": { ... star tree config ... }
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    static CompressedXContent buildStarTreeMappingSource(StarTreeUpgradeRequest request) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(JSON.xContent());
        builder.startObject();
        builder.startObject(MapperService.SINGLE_MAPPING_NAME);
        builder.startObject("composite");
        builder.startObject(request.getStarTreeField().getName());
        builder.field("type", "star_tree");
        builder.field("config", request.getStarTreeField());
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        return new CompressedXContent(builder.toString());
    }

    /**
     * Task object that carries the request and resolved indices for the mapping update.
     */
    static class StarTreeUpgradeMappingTask {
        final StarTreeUpgradeRequest request;
        final String[] concreteIndices;

        StarTreeUpgradeMappingTask(StarTreeUpgradeRequest request, String[] concreteIndices) {
            this.request = request;
            this.concreteIndices = concreteIndices;
        }

        @Override
        public String toString() {
            return "star-tree-upgrade-mapping";
        }
    }

    /**
     * Cluster state task executor that applies the star tree mapping update.
     * Follows the same pattern as {@code MetadataMappingService.PutMappingExecutor}
     * but sets the bypass flag on the parser context and uses
     * {@link MergeReason#STAR_TREE_UPGRADE}.
     */
    class StarTreeUpgradeMappingExecutor implements ClusterStateTaskExecutor<StarTreeUpgradeMappingTask> {

        @Override
        public ClusterTasksResult<StarTreeUpgradeMappingTask> execute(ClusterState currentState, List<StarTreeUpgradeMappingTask> tasks)
            throws Exception {
            Map<Index, MapperService> indexMapperServices = new HashMap<>();
            ClusterTasksResult.Builder<StarTreeUpgradeMappingTask> builder = ClusterTasksResult.builder();
            try {
                for (StarTreeUpgradeMappingTask task : tasks) {
                    try {
                        currentState = applyStarTreeMapping(currentState, task, indexMapperServices);
                        builder.success(task);
                    } catch (Exception e) {
                        builder.failure(task, e);
                    }
                }
                return builder.build(currentState);
            } finally {
                org.opensearch.common.util.io.IOUtils.close(indexMapperServices.values());
            }
        }

        private ClusterState applyStarTreeMapping(
            ClusterState currentState,
            StarTreeUpgradeMappingTask task,
            Map<Index, MapperService> indexMapperServices
        ) throws IOException {
            final Metadata metadata = currentState.metadata();

            for (String indexName : task.concreteIndices) {
                IndexMetadata indexMetadata = metadata.index(indexName);
                Index index = indexMetadata.getIndex();

                if (indexMapperServices.containsKey(index) == false) {
                    MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
                    // Set the bypass flag so parseCompositeField() skips the IS_COMPOSITE_INDEX_SETTING check
                    mapperService.documentMapperParser().setAllowCompositeFieldWithoutSettings(true);
                    indexMapperServices.put(index, mapperService);
                    // Merge existing mappings for cross-type validation
                    mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                }
            }

            Metadata.Builder metadataBuilder = Metadata.builder(metadata);
            boolean updated = false;

            for (String indexName : task.concreteIndices) {
                IndexMetadata indexMetadata = currentState.getMetadata().index(indexName);
                Index index = indexMetadata.getIndex();
                MapperService mapperService = indexMapperServices.get(index);

                boolean isCompositeFieldPresent = mapperService.getCompositeFieldTypes().isEmpty() == false;

                CompressedXContent existingSource = null;
                DocumentMapper existingMapper = mapperService.documentMapper();
                if (existingMapper != null) {
                    existingSource = existingMapper.mappingSource();
                }

                // Build complete mapping source (existing properties + new composite section)
                // so StarTreeMapper.Builder can look up dimension/metric fields.
                CompressedXContent mappingUpdateSource = buildCompleteMappingSource(task.request, indexMetadata);

                // Parse and merge the star tree mapping with STAR_TREE_UPGRADE merge reason
                DocumentMapper mergedMapper = mapperService.merge(
                    MapperService.SINGLE_MAPPING_NAME,
                    mappingUpdateSource,
                    MergeReason.STAR_TREE_UPGRADE
                );

                // Validate the composite field (dims/metrics against existing fields)
                // using the upgrade merge reason to bypass the "no new composite fields" restriction
                CompositeIndexValidator.validate(
                    mapperService,
                    indicesService.getCompositeIndexSettings(),
                    mapperService.getIndexSettings(),
                    isCompositeFieldPresent,
                    MergeReason.STAR_TREE_UPGRADE
                );

                CompressedXContent updatedSource = mergedMapper.mappingSource();

                boolean updatedMapping = false;
                if (existingSource != null) {
                    if (existingSource.equals(updatedSource) == false) {
                        updatedMapping = true;
                        logger.info("{} star_tree_upgrade update_mapping [{}]", index, mergedMapper.type());
                    }
                } else {
                    updatedMapping = true;
                    logger.info("{} star_tree_upgrade create_mapping", index);
                }

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                // Force-enable append_only and composite_index settings for star tree correctness.
                boolean needAppendOnly = IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.get(indexMetadata.getSettings()) == false;
                boolean needCompositeIndex = StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.get(indexMetadata.getSettings()) == false;
                if (needAppendOnly || needCompositeIndex) {
                    Settings.Builder settingsBuilder = Settings.builder().put(indexMetadata.getSettings());
                    if (needAppendOnly) {
                        settingsBuilder.put(IndexMetadata.SETTING_INDEX_APPEND_ONLY_ENABLED, true);
                    }
                    if (needCompositeIndex) {
                        settingsBuilder.put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true);
                    }
                    indexMetadataBuilder.settings(settingsBuilder.build());
                    indexMetadataBuilder.settingsVersion(1 + indexMetadata.getSettingsVersion());
                }
                DocumentMapper mapper = mapperService.documentMapper();
                if (mapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(mapper.mappingSource()));
                }
                if (updatedMapping) {
                    indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion());
                }
                metadataBuilder.put(indexMetadataBuilder);
                updated |= updatedMapping;
            }

            if (updated) {
                return ClusterState.builder(currentState).metadata(metadataBuilder).build();
            } else {
                return currentState;
            }
        }
    }
}
