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

package org.opensearch.indices.replication.copy;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardRelocatedException;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.DelayRecoveryException;
import org.opensearch.indices.recovery.MultiChunkTransfer;
import org.opensearch.indices.recovery.RemoteRecoveryTargetHandler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class PrimaryShardReplicationHandler {

    private final long replicationId;
    private final Logger logger;
    // Shard that is going to be recovered (the "source" on the current Node)
    private final IndexShard shard;
    private final String targetAllocationId;
    private final DiscoveryNode targetNode;
    private final int shardId;
    // Request containing source and target node information
    private final GetFilesRequest request;
    private final int chunkSizeInBytes;
    private final RemoteRecoveryTargetHandler targetClient;
    private final int maxConcurrentFileChunks;
    private final int maxConcurrentOperations;
    private final ThreadPool threadPool;
    private final CancellableThreads cancellableThreads = new CancellableThreads();
    private final List<Closeable> resources = new CopyOnWriteArrayList<>();
    private final ListenableFuture<GetFilesResponse> future = new ListenableFuture<>();

    public PrimaryShardReplicationHandler(
        long replicationId,
        IndexShard shard,
        DiscoveryNode targetNode,
        String targetAllocationId,
        RemoteRecoveryTargetHandler targetClient,
        ThreadPool threadPool,
        GetFilesRequest request,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks,
        int maxConcurrentOperations
    ) {
        this.replicationId = replicationId;
        this.shard = shard;
        this.targetNode = targetNode;
        this.targetAllocationId = targetAllocationId;
        this.targetClient = targetClient;
        this.threadPool = threadPool;
        this.request = request;
        this.shardId = shard.shardId().id();
        this.logger = Loggers.getLogger(getClass(), shard.shardId(), "recover to " + request.getTargetNode().getName());
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        // if the target is on an old version, it won't be able to handle out-of-order file chunks.
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
        this.maxConcurrentOperations = maxConcurrentOperations;
    }

    /**
     * performs the recovery from the local engine to the target
     */
    public void sendFiles(CopyState copyState, ActionListener<GetFilesResponse> listener) {
        future.addListener(listener, OpenSearchExecutors.newDirectExecutorService());
        final Closeable releaseResources = () -> IOUtils.close(resources);
        try {

            final Consumer<Exception> onFailure = e -> {
                assert Transports.assertNotTransportThread(PrimaryShardReplicationHandler.this + "[onFailure]");
                IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
            };

            runUnderPrimaryPermit(() -> {
                final IndexShardRoutingTable routingTable = shard.getReplicationGroup().getRoutingTable();
                ShardRouting targetShardRouting = routingTable.getByAllocationId(request.getTargetAllocationId());
                if (targetShardRouting == null) {
                    logger.debug(
                        "delaying replication of {} as it is not listed as assigned to target node {}",
                        shard.shardId(),
                        request.getTargetNode()
                    );
                    throw new DelayRecoveryException("source node does not have the shard listed in its state as allocated on the node");
                }
            },
                shardId + " validating recovery target [" + request.getTargetAllocationId() + "] registered ",
                shard,
                cancellableThreads,
                logger
            );

            final StepListener<Void> sendFileStep = new StepListener<>();
            try {
                Set<String> storeFiles = new HashSet<>(Arrays.asList(shard.store().directory().listAll()));
                final StoreFileMetadata[] storeFileMetadata = request.getFilesToFetch()
                    .stream()
                    .filter(file -> storeFiles.contains(file.name()))
                    .toArray(StoreFileMetadata[]::new);
                sendFiles(shard.store(), storeFileMetadata, sendFileStep);
            } catch (final Exception e) {
                throw new RecoveryEngineException(shard.shardId(), 1, "sendFileStep failed", e);
            }

            sendFileStep.whenComplete(r -> {
                runUnderPrimaryPermit(
                    () -> shard.updateLocalCheckpointForShard(targetAllocationId, request.getCheckpoint().getSeqNo()),
                    shardId + " updating local checkpoint for " + targetAllocationId,
                    shard,
                    cancellableThreads,
                    logger
                );
                runUnderPrimaryPermit(
                    () -> shard.markAllocationIdAsInSync(targetAllocationId, request.getCheckpoint().getSeqNo()),
                    shardId + " marking " + targetAllocationId + " as in sync",
                    shard,
                    cancellableThreads,
                    logger
                );
                try {
                    future.onResponse(new GetFilesResponse());
                } finally {
                    IOUtils.close(resources);
                }
            }, onFailure);
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(releaseResources, () -> future.onFailure(e));
        }
    }

    static void runUnderPrimaryPermit(
        CancellableThreads.Interruptible runnable,
        String reason,
        IndexShard primary,
        CancellableThreads cancellableThreads,
        Logger logger
    ) {
        cancellableThreads.execute(() -> {
            CompletableFuture<Releasable> permit = new CompletableFuture<>();
            final ActionListener<Releasable> onAcquired = new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    if (permit.complete(releasable) == false) {
                        releasable.close();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    permit.completeExceptionally(e);
                }
            };
            primary.acquirePrimaryOperationPermit(onAcquired, ThreadPool.Names.SAME, reason);
            try (Releasable ignored = FutureUtils.get(permit)) {
                // check that the IndexShard still has the primary authority. This needs to be checked under operation permit to prevent
                // races, as IndexShard will switch its authority only when it holds all operation permits, see IndexShard.relocated()
                if (primary.isRelocatedPrimary()) {
                    throw new IndexShardRelocatedException(primary.shardId());
                }
                runnable.run();
            } finally {
                // just in case we got an exception (likely interrupted) while waiting for the get
                permit.whenComplete((r, e) -> {
                    if (r != null) {
                        r.close();
                    }
                    if (e != null) {
                        logger.trace("suppressing exception on completion (it was already bubbled up or the operation was aborted)", e);
                    }
                });
            }
        });
    }

    /**
     * Cancels the recovery and interrupts all eligible threads.
     */
    public void cancel(String reason) {
        cancellableThreads.cancel(reason);
    }

    @Override
    public String toString() {
        return "SegmentReplicationSourceHandler{" + "shardId=" + shard.shardId() + ", targetNode=" + request.getTargetNode() + '}';
    }

    private static class FileChunk implements MultiChunkTransfer.ChunkRequest, Releasable {
        final StoreFileMetadata md;
        final BytesReference content;
        final long position;
        final boolean lastChunk;
        final Releasable onClose;

        FileChunk(StoreFileMetadata md, BytesReference content, long position, boolean lastChunk, Releasable onClose) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
            this.onClose = onClose;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }

        @Override
        public void close() {
            onClose.close();
        }
    }

    void sendFiles(Store store, StoreFileMetadata[] files, ActionListener<Void> listener) {

        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetadata::length)); // send smallest first

        final MultiChunkTransfer<StoreFileMetadata, FileChunk> multiFileSender = new MultiChunkTransfer<StoreFileMetadata, FileChunk>(
            logger,
            threadPool.getThreadContext(),
            listener,
            maxConcurrentFileChunks,
            Arrays.asList(files)
        ) {

            final Deque<byte[]> buffers = new ConcurrentLinkedDeque<>();
            InputStreamIndexInput currentInput = null;
            long offset = 0;

            @Override
            protected void onNewResource(StoreFileMetadata md) throws IOException {
                offset = 0;
                IOUtils.close(currentInput, () -> currentInput = null);
                final IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE);
                currentInput = new InputStreamIndexInput(indexInput, md.length()) {
                    @Override
                    public void close() throws IOException {
                        IOUtils.close(indexInput, super::close); // InputStreamIndexInput's close is a noop
                    }
                };
            }

            private byte[] acquireBuffer() {
                final byte[] buffer = buffers.pollFirst();
                if (buffer != null) {
                    return buffer;
                }
                return new byte[chunkSizeInBytes];
            }

            @Override
            protected FileChunk nextChunkRequest(StoreFileMetadata md) throws IOException {
                assert Transports.assertNotTransportThread("read file chunk");
                cancellableThreads.checkForCancel();
                final byte[] buffer = acquireBuffer();
                final int bytesRead = currentInput.read(buffer);
                if (bytesRead == -1) {
                    throw new CorruptIndexException("file truncated; length=" + md.length() + " offset=" + offset, md.name());
                }
                final boolean lastChunk = offset + bytesRead == md.length();
                final FileChunk chunk = new FileChunk(
                    md,
                    new BytesArray(buffer, 0, bytesRead),
                    offset,
                    lastChunk,
                    () -> buffers.addFirst(buffer)
                );
                offset += bytesRead;
                return chunk;
            }

            @Override
            protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
                cancellableThreads.checkForCancel();
                targetClient.writeFileChunk(
                    request.md,
                    request.position,
                    request.content,
                    request.lastChunk,
                    0,
                    ActionListener.runBefore(listener, request::close)
                );
            }

            @Override
            protected void handleError(StoreFileMetadata md, Exception e) throws Exception {
                handleErrorOnSendFiles(store, e, new StoreFileMetadata[] { md });
            }

            @Override
            public void close() throws IOException {
                IOUtils.close(currentInput, () -> currentInput = null);
            }
        };
        resources.add(multiFileSender);
        multiFileSender.start();
    }

    private void handleErrorOnSendFiles(Store store, Exception e, StoreFileMetadata[] mds) throws Exception {
        final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
        assert Transports.assertNotTransportThread(PrimaryShardReplicationHandler.this + "[handle error on send/clean files]");
        if (corruptIndexException != null) {
            Exception localException = null;
            for (StoreFileMetadata md : mds) {
                cancellableThreads.checkForCancel();
                logger.debug("checking integrity for file {} after remove corruption exception", md);
                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                    logger.warn("{} Corrupted file detected {} checksum mismatch", shardId, md);
                    if (localException == null) {
                        localException = corruptIndexException;
                    }
                    failEngine(corruptIndexException);
                }
            }
            if (localException != null) {
                throw localException;
            } else { // corruption has happened on the way to replica
                RemoteTransportException remoteException = new RemoteTransportException(
                    "File corruption occurred on recovery but checksums are ok",
                    null
                );
                remoteException.addSuppressed(e);
                logger.warn(
                    () -> new ParameterizedMessage(
                        "{} Remote file corruption on node {}, recovering {}. local checksum OK",
                        shardId,
                        request.getTargetNode(),
                        mds
                    ),
                    corruptIndexException
                );
                throw remoteException;
            }
        }
        throw e;
    }

    protected void failEngine(IOException cause) {
        shard.failShard("recovery", cause);
    }
}
