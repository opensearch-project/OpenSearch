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
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.DelayRecoveryException;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.MultiChunkTransfer;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.util.Arrays;
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
     * @param targetNode              - {@link DiscoveryNode} target node where files should be sent.
     * @param writer                  {@link FileChunkWriter} implementation that sends file chunks over the transport layer.
     * @param threadPool              {@link ThreadPool} Thread pool.
     * @param copyState               {@link CopyState} CopyState holding segment file metadata.
     * @param fileChunkSizeInBytes    {@link Integer}
     * @param maxConcurrentFileChunks {@link Integer}
     */
    SegmentReplicationSourceHandler(
        DiscoveryNode targetNode,
        FileChunkWriter writer,
        ThreadPool threadPool,
        CopyState copyState,
        String allocationId,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks
    ) {
        this.targetNode = targetNode;
        this.shard = copyState.getShard();
        this.logger = Loggers.getLogger(
            SegmentReplicationSourceHandler.class,
            copyState.getShard().shardId(),
            "sending segments to " + targetNode.getName()
        );
        this.segmentFileTransferHandler = new SegmentFileTransferHandler(
            copyState.getShard(),
            targetNode,
            writer,
            logger,
            threadPool,
            cancellableThreads,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks
        );
        this.allocationId = allocationId;
        this.copyState = copyState;
        this.writer = writer;
    }

    /**
     * Sends Segment files from the local node to the given target.
     *
     * @param request  {@link GetSegmentFilesRequest} request object containing list of files to be sent.
     * @param listener {@link ActionListener} that completes with the list of files sent.
     */
    public synchronized void sendFiles(GetSegmentFilesRequest request, ActionListener<GetSegmentFilesResponse> listener) {
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

            final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
            ShardRouting targetShardRouting = routingTable.getByAllocationId(request.getTargetAllocationId());
            if (targetShardRouting == null) {
                logger.debug("delaying replication of {} as it is not listed as assigned to target node {}", shard.shardId(), targetNode);
                throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
            }

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
                } finally {
                    IOUtils.close(resources);
                    timer.stop();
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
    }

    CopyState getCopyState() {
        return copyState;
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
}
