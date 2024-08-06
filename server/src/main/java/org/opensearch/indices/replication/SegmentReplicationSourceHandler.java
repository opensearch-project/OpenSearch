/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.MultiChunkTransfer;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Orchestrates sending requested segment files to a target shard.
 *
 * @opensearch.internal
 */
class SegmentReplicationSourceHandler {

    private final IndexShard shard;
    private final CopyState copyState;
    private final SegmentFileTransferHandler segmentFileTransferHandler;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final ListenableFuture<GetSegmentFilesResponse> future = new ListenableFuture<>();
    private final List<Closeable> resources = new CopyOnWriteArrayList<>();
    private final Logger logger;
    private final AtomicBoolean isReplicating = new AtomicBoolean();
    private final DiscoveryNode targetNode;
    private final String allocationId;
    private final FileChunkWriter writer;

    /**
     * Constructor.
     *
     * @param targetNode              {@link DiscoveryNode} target node where files should be sent.
     * @param writer                  {@link FileChunkWriter} implementation that sends file chunks over the transport layer.
     * @param shard                   {@link IndexShard} The primary shard local to this node.
     * @param fileChunkSizeInBytes    {@link Integer}
     * @param maxConcurrentFileChunks {@link Integer}
     */
    SegmentReplicationSourceHandler(
        DiscoveryNode targetNode,
        FileChunkWriter writer,
        IndexShard shard,
        String allocationId,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks
    ) throws IOException {
        this.targetNode = targetNode;
        this.shard = shard;
        this.logger = Loggers.getLogger(
            SegmentReplicationSourceHandler.class,
            shard.shardId(),
            "sending segments to " + targetNode.getName()
        );
        this.segmentFileTransferHandler = new SegmentFileTransferHandler(
            shard,
            targetNode,
            writer,
            logger,
            shard.getThreadPool(),
            cancellableThreads,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks
        );
        this.allocationId = allocationId;
        this.copyState = new CopyState(shard);
        this.writer = writer;
        resources.add(copyState);
    }

    /**
     * Sends Segment files from the local node to the given target.
     *
     * @param request  {@link GetSegmentFilesRequest} request object containing list of files to be sent.
     * @param listener {@link ActionListener} that completes with the list of files sent.
     */
    public synchronized void sendFiles(GetSegmentFilesRequest request, ActionListener<GetSegmentFilesResponse> listener) {
        // Short circuit when no files to transfer
        if (request.getFilesToFetch().isEmpty()) {
            // before completion, alert the primary of the replica's state.
            shard.updateVisibleCheckpointForShard(request.getTargetAllocationId(), copyState.getCheckpoint());
            IOUtils.closeWhileHandlingException(copyState);
            listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
            return;
        }

        final ReplicationTimer timer = new ReplicationTimer();
        if (isReplicating.compareAndSet(false, true) == false) {
            throw new OpenSearchException("Replication to {} is already running.", shard.shardId());
        }
        future.addListener(listener, OpenSearchExecutors.newDirectExecutorService());
        final Closeable releaseResources = () -> IOUtils.close(resources);
        try {
            timer.start();
            cancellableThreads.setOnCancel((reason, beforeCancelEx) -> {
                final RuntimeException e = new CancellableThreads.ExecutionCancelledException(
                    "replication was canceled reason [" + reason + "]"
                );
                if (beforeCancelEx != null) {
                    e.addSuppressed(beforeCancelEx);
                }
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
                throw e;
            });
            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(SegmentReplicationSourceHandler.this + "[onFailure]");
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
                timer.stop();
                logger.trace(
                    "[replication id {}] Source node failed to send files to target node [{}], timing: {}",
                    request.getReplicationId(),
                    request.getTargetNode().getId(),
                    timer.time()
                );
            };
            cancellableThreads.checkForCancel();

            final StepListener<Void> sendFileStep = new StepListener<>();
            Set<String> storeFiles = new HashSet<>(Arrays.asList(shard.store().directory().listAll()));
            final StoreFileMetadata[] storeFileMetadata = request.getFilesToFetch()
                .stream()
                .filter(file -> storeFiles.contains(file.name()))
                .toArray(StoreFileMetadata[]::new);

            final MultiChunkTransfer<StoreFileMetadata, SegmentFileTransferHandler.FileChunk> transfer = segmentFileTransferHandler
                .createTransfer(shard.store(), storeFileMetadata, () -> 0, sendFileStep);
            resources.add(transfer);
            cancellableThreads.checkForCancel();
            transfer.start();

            sendFileStep.whenComplete(r -> {
                try {
                    shard.updateVisibleCheckpointForShard(allocationId, copyState.getCheckpoint());
                    future.onResponse(new GetSegmentFilesResponse(List.of(storeFileMetadata)));
                    timer.stop();
                } finally {
                    IOUtils.close(resources);
                    logger.trace(
                        "[replication id {}] Source node completed sending files to target node [{}], timing: {}",
                        request.getReplicationId(),
                        request.getTargetNode().getId(),
                        timer.time()
                    );
                }
            }, onFailure);
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
        }
    }

    /**
     * Cancels the replication and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        writer.cancel();
        cancellableThreads.cancel(reason);
        IOUtils.closeWhileHandlingException(copyState);
    }

    public boolean isReplicating() {
        return isReplicating.get();
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public ReplicationCheckpoint getCheckpoint() {
        return copyState.getCheckpoint();
    }

    public byte[] getInfosBytes() {
        return copyState.getInfosBytes();
    }

    public ShardId shardId() {
        return shard.shardId();
    }
}
