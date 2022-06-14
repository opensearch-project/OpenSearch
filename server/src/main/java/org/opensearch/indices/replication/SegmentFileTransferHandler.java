/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.MultiChunkTransfer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transports;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.IntSupplier;

/**
 * SegmentFileSender handles building and starting a {@link MultiChunkTransfer} to orchestrate sending chunks to a given targetNode.
 * This class delegates to a {@link FileChunkWriter} to handle the transport of chunks.
 *
 * @opensearch.internal
 * // TODO: make this package-private after combining recovery and replication into single package.
 */
public final class SegmentFileTransferHandler {

    private final Logger logger;
    private final IndexShard shard;
    private final FileChunkWriter chunkWriter;
    private final ThreadPool threadPool;
    private final int chunkSizeInBytes;
    private final int maxConcurrentFileChunks;
    private final DiscoveryNode targetNode;
    private final CancellableThreads cancellableThreads;

    public SegmentFileTransferHandler(
        IndexShard shard,
        DiscoveryNode targetNode,
        FileChunkWriter chunkWriter,
        Logger logger,
        ThreadPool threadPool,
        CancellableThreads cancellableThreads,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks
    ) {
        this.shard = shard;
        this.targetNode = targetNode;
        this.chunkWriter = chunkWriter;
        this.logger = logger;
        this.threadPool = threadPool;
        this.cancellableThreads = cancellableThreads;
        this.chunkSizeInBytes = fileChunkSizeInBytes;
        // if the target is on an old version, it won't be able to handle out-of-order file chunks.
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    /**
     * Returns a closeable {@link MultiChunkTransfer} to initiate sending a list of files.
     * Callers are responsible for starting the transfer and closing the resource.
     * @param store {@link Store}
     * @param files {@link StoreFileMetadata[]}
     * @param translogOps {@link IntSupplier}
     * @param listener {@link ActionListener}
     * @return {@link MultiChunkTransfer}
     */
    public MultiChunkTransfer<StoreFileMetadata, FileChunk> createTransfer(
        Store store,
        StoreFileMetadata[] files,
        IntSupplier translogOps,
        ActionListener<Void> listener
    ) {
        ArrayUtil.timSort(files, Comparator.comparingLong(StoreFileMetadata::length)); // send smallest first
        return new MultiChunkTransfer<>(logger, threadPool.getThreadContext(), listener, maxConcurrentFileChunks, Arrays.asList(files)) {

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
            protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener1) {
                cancellableThreads.checkForCancel();
                chunkWriter.writeFileChunk(
                    request.md,
                    request.position,
                    request.content,
                    request.lastChunk,
                    translogOps.getAsInt(),
                    ActionListener.runBefore(listener1, request::close)
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
    }

    public void handleErrorOnSendFiles(Store store, Exception e, StoreFileMetadata[] mds) throws Exception {
        final IOException corruptIndexException = ExceptionsHelper.unwrapCorruption(e);
        assert Transports.assertNotTransportThread(this + "[handle error on send/clean files]");
        if (corruptIndexException != null) {
            Exception localException = null;
            for (StoreFileMetadata md : mds) {
                cancellableThreads.checkForCancel();
                logger.debug("checking integrity for file {} after remove corruption exception", md);
                if (store.checkIntegrityNoException(md) == false) { // we are corrupted on the primary -- fail!
                    logger.warn("{} Corrupted file detected {} checksum mismatch", shard.shardId(), md);
                    if (localException == null) {
                        localException = corruptIndexException;
                    }
                    shard.failShard("error sending files", corruptIndexException);
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
                        shard.shardId(),
                        targetNode,
                        mds
                    ),
                    corruptIndexException
                );
                throw remoteException;
            }
        }
        throw e;
    }

    /**
     * A file chunk from the recovery source
     *
     * @opensearch.internal
     */
    public static final class FileChunk implements MultiChunkTransfer.ChunkRequest, Releasable {
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
}
