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
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.index.translog.transfer.FileTransferTracker;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
            state.blocks().globalBlockedRaiseException(ClusterBlockLevel.METADATA_READ);
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);

            if (concreteIndices.length == 0) {
                listener.onResponse(new RemoteStoreMetadataResponse(new RemoteStoreMetadata[0], 0, 0, 0, Collections.emptyList()));
                return;
            }

            List<RemoteStoreMetadata> responses = Collections.synchronizedList(new ArrayList<>());
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
                    int[] shardIds = request.shards().length == 0
                        ? new int[indexMetadata.getNumberOfShards()]
                        : Arrays.stream(request.shards()).mapToInt(Integer::parseInt).toArray();
                    for (int shardId : shardIds) {
                        failedShards.incrementAndGet();
                        shardFailures.add(new DefaultShardOperationFailedException(
                            indexName,
                            shardId,
                            new IllegalStateException("Remote store not enabled for index")
                        ));
                    }
                    continue;
                }

                Index index = indexMetadata.getIndex();
                int[] shardIds = request.shards().length == 0
                    ? new int[indexMetadata.getNumberOfShards()]
                    : Arrays.stream(request.shards()).mapToInt(Integer::parseInt).toArray();

                for (int shardId : shardIds) {
                    ShardId sid = new ShardId(index, shardId);
                    IndexService indexService = indicesService.indexService(index);
                    Map<String, Map<String, Object>> segmentMetadataFiles = getSegmentMetadata(
                        remoteDirectoryFactory,
                        indexMetadata,
                        index,
                        sid,
                        indexService
                    );

                    String latestSegmentMetadataFilename = segmentMetadataFiles.isEmpty()
                        ? null
                        : new ArrayList<>(segmentMetadataFiles.keySet()).get(0);

                    getTranslogMetadataFiles(indexMetadata, sid, indexService, ActionListener.wrap(translogMetadataFiles -> {
                        String latestTranslogMetadataFilename = translogMetadataFiles.isEmpty()
                            ? null
                            : new ArrayList<>(translogMetadataFiles.keySet()).get(0);

                        responses.add(new RemoteStoreMetadata(
                            indexName,
                            shardId,
                            segmentMetadataFiles,
                            translogMetadataFiles,
                            latestSegmentMetadataFilename,
                            latestTranslogMetadataFilename
                        ));
                        successfulShards.incrementAndGet();
                        if (successfulShards.get() + failedShards.get() == shardIds.length * concreteIndices.length) {
                            finalizeResponse(listener, responses, successfulShards, failedShards, shardFailures);
                        }
                    }, e -> {
                        failedShards.incrementAndGet();
                        shardFailures.add(new DefaultShardOperationFailedException(indexName, shardId, e));
                        logger.warn("Failed to fetch translog metadata for index [{}] shard [{}]", indexName, shardId, e);

                        if (successfulShards.get() + failedShards.get() == shardIds.length * concreteIndices.length) {
                            finalizeResponse(listener, responses, successfulShards, failedShards, shardFailures);
                        }
                    }));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to execute remote store metadata action", e);
            listener.onFailure(e);
        }
    }


    private void finalizeResponse(
        ActionListener<RemoteStoreMetadataResponse> listener,
        List<RemoteStoreMetadata> responses,
        AtomicInteger successfulShards,
        AtomicInteger failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        listener.onResponse(
            new RemoteStoreMetadataResponse(
                responses.toArray(new RemoteStoreMetadata[0]),
                successfulShards.get() + failedShards.get(),
                successfulShards.get(),
                failedShards.get(),
                shardFailures
            )
        );
    }

    private Map<String, Map<String, Object>> getSegmentMetadata(
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

        Map<String, RemoteSegmentMetadata> segmentMetadataMapWithFilenames = remoteDirectory.readLatestNMetadataFiles(5);
        Map<String, Map<String, Object>> metadataFilesMap = new LinkedHashMap<>();

        for (Map.Entry<String, RemoteSegmentMetadata> entry : segmentMetadataMapWithFilenames.entrySet()) {
            String fileName = entry.getKey();
            RemoteSegmentMetadata segmentMetadata = entry.getValue();

            Map<String, Object> segmentMetadataMap = new HashMap<>();
            Map<String, Object> filesMap = new HashMap<>();
            segmentMetadata.getMetadata().forEach((file, meta) -> {
                Map<String, Object> metaMap = new HashMap<>();
                metaMap.put("original_name", meta.getOriginalFilename());
                metaMap.put("checksum", meta.getChecksum());
                metaMap.put("length", meta.getLength());
                filesMap.put(file, metaMap);
            });
            segmentMetadataMap.put("files", filesMap);

            ReplicationCheckpoint checkpoint = segmentMetadata.getReplicationCheckpoint();
            if (checkpoint != null) {
                Map<String, Object> checkpointMap = new HashMap<>();
                checkpointMap.put("primary_term", checkpoint.getPrimaryTerm());
                checkpointMap.put("segments_gen", checkpoint.getSegmentsGen());
                checkpointMap.put("segment_infos_version", checkpoint.getSegmentInfosVersion());
                checkpointMap.put("length", checkpoint.getLength());
                checkpointMap.put("codec", checkpoint.getCodec());
                checkpointMap.put("created_timestamp", checkpoint.getCreatedTimeStamp());
                segmentMetadataMap.put("replication_checkpoint", checkpointMap);
            }
            metadataFilesMap.put(fileName, segmentMetadataMap);
        }
        return metadataFilesMap;
    }

    private void getTranslogMetadataFiles(
        IndexMetadata indexMetadata,
        ShardId shardId,
        IndexService indexService,
        ActionListener<Map<String, Map<String, Object>>> listener
    ) {
        String repository = IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.get(indexMetadata.getSettings());
        if (repository == null) {
            listener.onResponse(Collections.emptyMap());
            return;
        }

        RemoteTranslogTransferTracker tracker = new RemoteTranslogTransferTracker(shardId, 1000);
        FileTransferTracker fileTransferTracker = new FileTransferTracker(shardId, tracker);
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repositoriesService.repository(repository);
        TranslogTransferManager manager = RemoteFsTranslog.buildTranslogTransferManager(
            blobStoreRepository,
            threadPool,
            shardId,
            fileTransferTracker,
            tracker,
            indexService.getIndexSettings().getRemoteStorePathStrategy(),
            new RemoteStoreSettings(clusterService.getSettings(), clusterService.getClusterSettings()),
            RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.get(indexMetadata.getSettings())
        );

        manager.readLatestNMetadataFiles(5, ActionListener.wrap(metadataMap -> {
            Map<String, Map<String, Object>> translogFilesMap = new LinkedHashMap<>();
            for (Map.Entry<String, TranslogTransferMetadata> entry : metadataMap.entrySet()) {
                String fileName = entry.getKey();
                TranslogTransferMetadata metadata = entry.getValue();

                Map<String, Object> fileMap = new HashMap<>();
                fileMap.put("primary_term", metadata.getPrimaryTerm());
                fileMap.put("generation", metadata.getGeneration());
                fileMap.put("min_translog_gen", metadata.getMinTranslogGeneration());

                Map<String, String> genToTerm = metadata.getGenerationToPrimaryTermMapper();
                if (genToTerm == null || genToTerm.isEmpty()) {
                    genToTerm = Map.of(String.valueOf(metadata.getGeneration()), String.valueOf(metadata.getPrimaryTerm()));
                }
                fileMap.put("generation_to_primary_term", genToTerm);

                translogFilesMap.put(fileName, fileMap);
            }
            listener.onResponse(translogFilesMap);
        }, listener::onFailure));
    }
}
