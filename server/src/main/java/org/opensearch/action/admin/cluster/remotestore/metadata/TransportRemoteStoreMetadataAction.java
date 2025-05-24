/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.index.IndexService;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tasks.Task;
import org.opensearch.core.index.Index;
import org.opensearch.transport.TransportService;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transport action responsible for collecting segment and translog metadata
 * from all shards of a given index.
 *
 * @opensearch.internal
 */
public class TransportRemoteStoreMetadataAction extends TransportAction<RemoteStoreMetadataRequest, RemoteStoreMetadataResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRemoteStoreMetadataAction.class);
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final RemoteStoreSettings remoteStoreSettings;

    @Inject
    public TransportRemoteStoreMetadataAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        RemoteStoreSettings remoteStoreSettings
    ) {
        super(RemoteStoreMetadataAction.NAME, actionFilters, transportService.getTaskManager());
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.remoteStoreSettings = remoteStoreSettings;
    }

    @Override
    protected void doExecute(Task task, RemoteStoreMetadataRequest request, ActionListener<RemoteStoreMetadataResponse> listener) {
        try {
            ClusterState state = clusterService.state();
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
            if (concreteIndices.length == 0) {
                listener.onResponse(new RemoteStoreMetadataResponse(new RemoteStoreMetadata[0], 0, 0, 0, Collections.emptyList()));
                return;
            }

            List<RemoteStoreMetadata> responses = new ArrayList<>();
            AtomicInteger successfulShards = new AtomicInteger(0);
            AtomicInteger failedShards = new AtomicInteger(0);
            List<DefaultShardOperationFailedException> shardFailures = Collections.synchronizedList(new ArrayList<>());
            RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory = new RemoteSegmentStoreDirectoryFactory(
                () -> repositoriesService, 
                threadPool, 
                remoteStoreSettings.getSegmentsPathFixedPrefix()
            );

            for (String indexName : concreteIndices) {
                IndexMetadata indexMetadata = state.metadata().index(indexName);
                if (!indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false)) {
                    int[] shardIds = request.shards().length == 0 ? new int[indexMetadata.getNumberOfShards()] : Arrays.stream(request.shards()).mapToInt(Integer::parseInt).toArray();
                    for (int shardId : shardIds) {
                        failedShards.incrementAndGet();
                        shardFailures.add(new DefaultShardOperationFailedException(indexName, shardId, new IllegalStateException("Remote store not enabled for index")));
                    }
                    continue;
                }

                Index index = indexMetadata.getIndex();
                int[] shardIds = request.shards().length == 0 ?
                    new int[indexMetadata.getNumberOfShards()] :
                    Arrays.stream(request.shards()).mapToInt(Integer::parseInt).toArray();

                for (int shardId : shardIds) {
                    try {
                        ShardId sid = new ShardId(index, shardId);
                        IndexService indexService = indicesService.indexService(index);

                        Map<String, Object> segmentMetadataMap = getSegmentMetadata(remoteDirectoryFactory, indexMetadata, index, sid, indexService);
                        Map<String, Object> translogMetadataMap = getTranslogMetadata(indexMetadata, sid, indexService);

                        responses.add(new RemoteStoreMetadata(segmentMetadataMap, translogMetadataMap, indexName, shardId));
                        successfulShards.incrementAndGet();
                    } catch (Exception e) {
                        failedShards.incrementAndGet();
                        shardFailures.add(new DefaultShardOperationFailedException(indexName, shardId, e));
                        logger.warn("Failed to fetch remote store metadata for index [{}] shard [{}]", indexName, shardId, e);
                    }
                }
            }

            listener.onResponse(new RemoteStoreMetadataResponse(
                responses.toArray(new RemoteStoreMetadata[0]),
                successfulShards.get() + failedShards.get(),
                successfulShards.get(),
                failedShards.get(),
                shardFailures
            ));

        } catch (Exception e) {
            logger.error("Failed to execute remote store metadata action", e);
            listener.onFailure(e);
        }
    }

    private Map<String, Object> getSegmentMetadata(
        RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory,
        IndexMetadata indexMetadata,
        Index index,
        ShardId shardId,
        IndexService indexService
    ) throws IOException {
        RemoteSegmentStoreDirectory remoteDirectory = (RemoteSegmentStoreDirectory) remoteDirectoryFactory.newDirectory(
            IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.get(indexMetadata.getSettings()),
            index.getUUID(),
            shardId,
            indexService.getIndexSettings().getRemoteStorePathStrategy()
        );

        RemoteSegmentMetadata segmentMetadata = remoteDirectory.readLatestMetadataFile();
        Map<String, Object> segmentMetadataMap = new HashMap<>();
        
        segmentMetadata.getMetadata().forEach((file, meta) -> {
            Map<String, Object> metaMap = new HashMap<>();
            metaMap.put("original_name", meta.getOriginalFilename());
            metaMap.put("checksum", meta.getChecksum());
            metaMap.put("length", meta.getLength());
            segmentMetadataMap.put(file, metaMap);
        });

        return segmentMetadataMap;
    }

    private Map<String, Object> getTranslogMetadata(
        IndexMetadata indexMetadata,
        ShardId shardId,
        IndexService indexService
    ) throws IOException {
        Map<String, Object> translogMetadataMap = new HashMap<>();
        String repository = IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.get(
            indexMetadata.getSettings()
        );
        
        if (repository != null) {
            RemoteTranslogTransferTracker remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 1000);
            FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker);
            BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repositoriesService.repository(repository);
            TranslogTransferManager transferManager = RemoteFsTranslog.buildTranslogTransferManager(
                blobStoreRepository,
                threadPool,
                shardId,
                fileTransferTracker,
                remoteTranslogTransferTracker,
                indexService.getIndexSettings().getRemoteStorePathStrategy(),
                new RemoteStoreSettings(clusterService.getSettings(), clusterService.getClusterSettings()),
                RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.get(indexMetadata.getSettings())
            );

            TranslogTransferMetadata translogMetadata = transferManager.readMetadata();
            if (translogMetadata != null) {
                translogMetadataMap.put("primary_term", translogMetadata.getPrimaryTerm());
                translogMetadataMap.put("generation", translogMetadata.getGeneration());
                translogMetadataMap.put("min_translog_gen", translogMetadata.getMinTranslogGeneration());
                translogMetadataMap.put("generation_to_primary_term", translogMetadata.getGenerationToPrimaryTermMapper());
            }
        }
        
        return translogMetadataMap;
    }
}
