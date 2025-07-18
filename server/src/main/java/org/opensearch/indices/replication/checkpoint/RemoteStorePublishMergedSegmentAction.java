/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.ActiveMergesRegistry;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Replication action responsible for publishing merged segment to a remote store and
 * notifying all replicas of newly available segments.
 *
 * @opensearch.api
 */
public class RemoteStorePublishMergedSegmentAction extends AbstractPublishCheckpointAction<
    RemoteStorePublishMergedSegmentRequest,
    RemoteStorePublishMergedSegmentRequest> implements MergedSegmentPublisher.PublishAction {

    public static final String ACTION_NAME = "indices:admin/remote_publish_merged_segment";

    private final static Logger logger = LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class);

    private final SegmentReplicationTargetService replicationService;

    @Inject
    public RemoteStorePublishMergedSegmentAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentReplicationTargetService targetService
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            RemoteStorePublishMergedSegmentRequest::new,
            RemoteStorePublishMergedSegmentRequest::new,
            ThreadPool.Names.GENERIC,
            logger
        );
        this.replicationService = targetService;
    }

    @Override
    protected void doReplicaOperation(RemoteStorePublishMergedSegmentRequest shardRequest, IndexShard replica) {
        RemoteStoreMergedSegmentCheckpoint checkpoint = shardRequest.getMergedSegment();
        if (checkpoint.getShardId().equals(replica.shardId())) {
            if (Objects.equals(checkpoint.getPrimaryTerm(), replica.getOperationPrimaryTerm()) == false) {
                logger.warn(() -> new ParameterizedMessage("Skipping checkpoint {} updated primary term.", checkpoint));
                return;
            }
            updateActiveMergesRegistry(checkpoint, replica.getRemoteDirectory().getActiveMergesRegistry());
            replicationService.onNewMergedSegmentCheckpoint(checkpoint, replica);
        }
    }

    @Override
    protected void shardOperationOnPrimary(
        RemoteStorePublishMergedSegmentRequest shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<RemoteStorePublishMergedSegmentRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(shardRequest, new ReplicationResponse()));
    }

    @Override
    public void publish(IndexShard indexShard, ReplicationCheckpoint checkpoint) {
        if (!(checkpoint instanceof MergedSegmentCheckpoint mergedSegmentCheckpoint)) {
            throw new AssertionError("Expected checkpoint to be an instance of " + MergedSegmentCheckpoint.class);
        }

        long startTimeSecs = System.currentTimeMillis() / 1000;
        long endTimeSecs;
        Map<String, String> localToRemoteStoreFilenames;
        try {
            localToRemoteStoreFilenames = uploadMergedSegmentsToRemoteStore(indexShard, mergedSegmentCheckpoint);
        } finally {
            endTimeSecs = System.currentTimeMillis() / 1000;
        }

        long elapsedTimeSecs = endTimeSecs - startTimeSecs;
        long timeoutSecs = indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().seconds();
        long timeLeftSecs = Math.max(0, timeoutSecs - elapsedTimeSecs);

        if (timeLeftSecs > 0) {
            RemoteStoreMergedSegmentCheckpoint remoteStoreMergedSegmentCheckpoint = new RemoteStoreMergedSegmentCheckpoint(
                mergedSegmentCheckpoint,
                localToRemoteStoreFilenames
            );
            doPublish(
                indexShard,
                remoteStoreMergedSegmentCheckpoint,
                new RemoteStorePublishMergedSegmentRequest(remoteStoreMergedSegmentCheckpoint),
                "segrep_remote_publish_merged_segment",
                true,
                TimeValue.timeValueSeconds(timeLeftSecs)
            );
        } else {
            logger.warn(
                () -> new ParameterizedMessage(
                    "Unable to confirm upload of merged segment {} to remote store. Timeout of {}s exceeded. Skipping pre-copy.",
                    mergedSegmentCheckpoint,
                    timeoutSecs
                )
            );
        }
    }

    private void updateActiveMergesRegistry(RemoteStoreMergedSegmentCheckpoint checkpoint, ActiveMergesRegistry registry) {
        final Map<String, String> localToRemoteFilenameMap = checkpoint.getLocalToRemoteSegmentFilenameMap();
        checkpoint.getMetadataMap()
            .forEach(
                ((localFilename, storeFileMetadata) -> registry.register(
                    localFilename,
                    new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(
                        localFilename,
                        localToRemoteFilenameMap.get(localFilename),
                        storeFileMetadata.checksum(),
                        storeFileMetadata.length()
                    )
                ))
            );
    }

    private Map<String, String> uploadMergedSegmentsToRemoteStore(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        Collection<String> segmentsToUpload = checkpoint.getMetadataMap().keySet();
        Map<String, String> localToRemoteStoreFilenames = new ConcurrentHashMap<>();
        ActiveMergesRegistry activeMergesRegistry = indexShard.getRemoteDirectory().getActiveMergesRegistry();

        Map<String, Long> segmentsSizeMap = checkpoint.getMetadataMap()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().length()));
        final CountDownLatch latch = new CountDownLatch(1);

        indexShard.getRemoteStoreUploaderService().uploadSegments(segmentsToUpload, segmentsSizeMap, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                if (logger.isTraceEnabled() == true) {
                    logger.trace(() -> new ParameterizedMessage("Successfully uploaded segments {} to remote store", segmentsToUpload));
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("Failed to upload segments {} to remote store. {}", segmentsToUpload, e));
                latch.countDown();
            }
        }, (x) -> new UploadListener() {
            @Override
            public void beforeUpload(String file) {
                activeMergesRegistry.register(file);
            }

            @Override
            public void onSuccess(String file) {
                localToRemoteStoreFilenames.put(file, activeMergesRegistry.get(file).getMetadata().getUploadedFilename());
            }

            @Override
            public void onFailure(String file) {
                logger.warn("Unable to upload segments during merge. Continuing.");
                activeMergesRegistry.unregister(file);
            }
        }, true);
        try {
            long timeout = indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().seconds();
            if (latch.await(timeout, TimeUnit.SECONDS) == false) {
                logger.warn("Unable to confirm successful merge segment downloads by replicas. Continuing.");
            }
        } catch (InterruptedException e) {
            logger.warn("Unable to confirm successful merge segment downloads by replicas. Continuing.");
        }

        return localToRemoteStoreFilenames;
    }
}
