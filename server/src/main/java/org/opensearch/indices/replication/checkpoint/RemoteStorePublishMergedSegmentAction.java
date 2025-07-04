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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.RemoteStoreUploaderService;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.ActiveMergesSegmentRegistry;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RemoteStorePublishMergedSegmentAction extends AbstractPublishCheckpointAction<
    RemoteStorePublishMergedSegmentRequest,
    RemoteStorePublishMergedSegmentRequest> implements MergedSegmentPublisher.PublishAction {

    public static final String ACTION_NAME = "indices:admin/remote_publish_merged_segment";

    private final static Logger logger = LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class);

    private final ActiveMergesSegmentRegistry activeMergesSegmentRegistry = ActiveMergesSegmentRegistry.getInstance();

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
        if (shardRequest.getMergedSegment().getShardId().equals(replica.shardId())) {
            replicationService.onNewMergedSegmentCheckpoint(shardRequest.getMergedSegment(), replica);
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
        if (!(checkpoint instanceof RemoteStoreMergedSegmentCheckpoint mergedSegmentCheckpoint)) {
            throw new AssertionError("Expected checkpoint to be an instance of " + RemoteStoreMergedSegmentCheckpoint.class);
        }

        publishMergedSegmentsToRemoteStore(indexShard, mergedSegmentCheckpoint);
        doPublish(
            indexShard,
            checkpoint,
            new RemoteStorePublishMergedSegmentRequest(mergedSegmentCheckpoint),
            "segrep_publish_merged_segment",
            true,
            indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout()
        );
    }

    private void publishMergedSegmentsToRemoteStore(IndexShard indexShard, RemoteStoreMergedSegmentCheckpoint checkpoint) {
        RemoteStoreUploaderService remoteStoreUploaderService = getRemoteStoreUploaderService(indexShard);
        Collection<String> segmentsToUpload = checkpoint.getMetadataMap().keySet();

        Map<String, Long> segmentsSizeMap = checkpoint.getMetadataMap()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().length()));

        final CountDownLatch latch = new CountDownLatch(segmentsToUpload.size());

        // TODO: Upload in low priority
        remoteStoreUploaderService.uploadSegments(segmentsToUpload, segmentsSizeMap, new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                if (logger.isTraceEnabled() == true) {
                    logger.trace("Successfully uploaded segments {} to remote store", segmentsToUpload);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to upload segments {} to remote store", segmentsToUpload, e);
                segmentsToUpload.forEach(activeMergesSegmentRegistry::unregister);
                throw new RuntimeException(e);
            }
        }, (x) -> new UploadListener() {
            @Override
            public void beforeUpload(String file) {
                activeMergesSegmentRegistry.register(file);
            }

            @Override
            public void onSuccess(String file) {
                checkpoint.updateLocalToRemoteSegmentFilenameMap(file, activeMergesSegmentRegistry.getExistingRemoteSegmentFilename(file));
                latch.countDown();
            }

            @Override
            public void onFailure(String file) {
                segmentsToUpload.forEach(activeMergesSegmentRegistry::unregister);
                /**
                 * TODO: abort merge
                 */
            }
        });
        try {
            if (latch.await(60, TimeUnit.MINUTES) == false) {
                throw new RuntimeException("Merged segment upload timed out.");
            }
            ; // TODO: Finalize timeout
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
            // TODO: abort merge properly here
        }
    }

    /**
     * TODO: REBASE ONCE UPLOAD CHANGES ARE COMPLETE
     */
    private RemoteStoreUploaderService getRemoteStoreUploaderService(IndexShard indexShard) {
        return new RemoteStoreUploaderService(indexShard, indexShard.store().directory(), indexShard.getRemoteDirectory());
    }
}
