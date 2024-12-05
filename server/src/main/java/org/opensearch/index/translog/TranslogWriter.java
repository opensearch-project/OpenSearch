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

package org.opensearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.opensearch.common.Nullable;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.Channels;
import org.opensearch.common.io.DiskIoBufferPool;
import org.opensearch.common.io.stream.ReleasableBytesStreamOutput;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.Assertions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Writer that writes operations to the translog
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TranslogWriter extends BaseTranslogReader implements Closeable {

    private final ShardId shardId;
    private final FileChannel checkpointChannel;
    private final Path checkpointPath;
    private final BigArrays bigArrays;
    // the last checkpoint that was written when the translog was last synced
    private volatile Checkpoint lastSyncedCheckpoint;
    /* the number of translog operations written to this file */
    private volatile int operationCounter;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private final TragicExceptionHolder tragedy;
    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private volatile long totalOffset;

    private volatile long minSeqNo;
    private volatile long maxSeqNo;

    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier minTranslogGenerationSupplier;

    // callback that's called whenever an operation with a given sequence number is successfully persisted.
    private final LongConsumer persistedSequenceNumberConsumer;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order try(Releasable lock = writeLock.acquire()) -> synchronized(this)
    private final ReleasableLock writeLock = new ReleasableLock(new ReentrantLock());
    // lock order synchronized(syncLock) -> try(Releasable lock = writeLock.acquire()) -> synchronized(this)
    private final Object syncLock = new Object();

    private List<Long> nonFsyncedSequenceNumbers = new ArrayList<>(64);
    private final int forceWriteThreshold;
    private volatile long bufferedBytes;
    private ReleasableBytesStreamOutput buffer;

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    @Nullable
    private final TranslogCheckedContainer translogCheckedContainer;

    private final Boolean remoteTranslogEnabled;

    private TranslogWriter(
        final ShardId shardId,
        final Checkpoint initialCheckpoint,
        final FileChannel channel,
        final FileChannel checkpointChannel,
        final Path path,
        final Path checkpointPath,
        final ByteSizeValue bufferSize,
        final LongSupplier globalCheckpointSupplier,
        LongSupplier minTranslogGenerationSupplier,
        TranslogHeader header,
        final TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer,
        final BigArrays bigArrays,
        TranslogCheckedContainer translogCheckedContainer,
        Boolean remoteTranslogEnabled
    ) throws IOException {
        super(initialCheckpoint.generation, channel, path, header);
        assert initialCheckpoint.offset == channel.position() : "initial checkpoint offset ["
            + initialCheckpoint.offset
            + "] is different than current channel position ["
            + channel.position()
            + "]";
        this.forceWriteThreshold = Math.toIntExact(bufferSize.getBytes());
        this.shardId = shardId;
        this.checkpointChannel = checkpointChannel;
        this.checkpointPath = checkpointPath;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = initialCheckpoint.offset;
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        this.minSeqNo = initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
        this.maxSeqNo = initialCheckpoint.maxSeqNo;
        assert initialCheckpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : initialCheckpoint.trimmedAboveSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.persistedSequenceNumberConsumer = persistedSequenceNumberConsumer;
        this.bigArrays = bigArrays;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
        this.tragedy = tragedy;
        this.translogCheckedContainer = translogCheckedContainer;
        this.remoteTranslogEnabled = remoteTranslogEnabled;
    }

    public static TranslogWriter create(
        ShardId shardId,
        String translogUUID,
        long fileGeneration,
        Path file,
        ChannelFactory channelFactory,
        ByteSizeValue bufferSize,
        final long initialMinTranslogGen,
        long initialGlobalCheckpoint,
        final LongSupplier globalCheckpointSupplier,
        final LongSupplier minTranslogGenerationSupplier,
        final long primaryTerm,
        TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer,
        final BigArrays bigArrays,
        Boolean remoteTranslogEnabled
    ) throws IOException {
        final Path checkpointFile = file.getParent().resolve(Translog.CHECKPOINT_FILE_NAME);

        final FileChannel channel = channelFactory.open(file);
        FileChannel checkpointChannel = null;
        try {
            checkpointChannel = channelFactory.open(checkpointFile, StandardOpenOption.WRITE);
            final TranslogHeader header = new TranslogHeader(translogUUID, primaryTerm);
            header.write(channel, !Boolean.TRUE.equals(remoteTranslogEnabled));
            TranslogCheckedContainer translogCheckedContainer = null;
            if (Boolean.TRUE.equals(remoteTranslogEnabled)) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                header.write(byteArrayOutputStream);
                translogCheckedContainer = new TranslogCheckedContainer(byteArrayOutputStream.toByteArray());
            }
            final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(
                header.sizeInBytes(),
                fileGeneration,
                initialGlobalCheckpoint,
                initialMinTranslogGen
            );
            writeCheckpoint(checkpointChannel, checkpointFile, checkpoint, remoteTranslogEnabled);
            final LongSupplier writerGlobalCheckpointSupplier;
            if (Assertions.ENABLED) {
                writerGlobalCheckpointSupplier = () -> {
                    long gcp = globalCheckpointSupplier.getAsLong();
                    assert gcp >= initialGlobalCheckpoint || (remoteTranslogEnabled == Boolean.TRUE) : "global checkpoint ["
                        + gcp
                        + "] lower than initial gcp ["
                        + initialGlobalCheckpoint
                        + "]";
                    return gcp;
                };
            } else {
                writerGlobalCheckpointSupplier = globalCheckpointSupplier;
            }
            return new TranslogWriter(
                shardId,
                checkpoint,
                channel,
                checkpointChannel,
                file,
                checkpointFile,
                bufferSize,
                writerGlobalCheckpointSupplier,
                minTranslogGenerationSupplier,
                header,
                tragedy,
                persistedSequenceNumberConsumer,
                bigArrays,
                translogCheckedContainer,
                remoteTranslogEnabled
            );
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation
            // is an error condition
            IOUtils.closeWhileHandlingException(channel, checkpointChannel);
            throw exception;
        }
    }

    private synchronized void closeWithTragicEvent(final Exception ex) {
        tragedy.setTragicException(ex);
        try {
            close();
        } catch (final IOException | RuntimeException e) {
            ex.addSuppressed(e);
        }
    }

    /**
     * Add the given bytes to the translog with the specified sequence number; returns the location the bytes were written to.
     *
     * @param data  the bytes to write
     * @param seqNo the sequence number associated with the operation
     * @return the location the bytes were written to
     * @throws IOException if writing to the translog resulted in an I/O exception
     */
    public Translog.Location add(final BytesReference data, final long seqNo) throws IOException {
        long bufferedBytesBeforeAdd = this.bufferedBytes;
        if (bufferedBytesBeforeAdd >= forceWriteThreshold) {
            writeBufferedOps(Long.MAX_VALUE, bufferedBytesBeforeAdd >= forceWriteThreshold * 4);
        }

        final Translog.Location location;
        synchronized (this) {
            ensureOpen();
            if (buffer == null) {
                buffer = new ReleasableBytesStreamOutput(bigArrays);
            }
            assert bufferedBytes == buffer.size();
            final long offset = totalOffset;
            totalOffset += data.length();
            data.writeTo(buffer);

            assert minSeqNo != SequenceNumbers.NO_OPS_PERFORMED || operationCounter == 0;
            assert maxSeqNo != SequenceNumbers.NO_OPS_PERFORMED || operationCounter == 0;

            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);

            nonFsyncedSequenceNumbers.add(seqNo);

            operationCounter++;

            assert assertNoSeqNumberConflict(seqNo, data);

            location = new Translog.Location(generation, offset, data.length());
            bufferedBytes = buffer.size();
        }

        return location;
    }

    private synchronized boolean assertNoSeqNumberConflict(long seqNo, BytesReference data) throws IOException {
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            // nothing to do
        } else if (seenSequenceNumbers.containsKey(seqNo)) {
            final Tuple<BytesReference, Exception> previous = seenSequenceNumbers.get(seqNo);
            if (previous.v1().equals(data) == false) {
                Translog.Operation newOp = Translog.readOperation(new BufferedChecksumStreamInput(data.streamInput(), "assertion"));
                Translog.Operation prvOp = Translog.readOperation(
                    new BufferedChecksumStreamInput(previous.v1().streamInput(), "assertion")
                );
                // TODO: We haven't had timestamp for Index operations in Lucene yet, we need to loosen this check without timestamp.
                final boolean sameOp;
                if (newOp instanceof Translog.Index && prvOp instanceof Translog.Index) {
                    final Translog.Index o1 = (Translog.Index) prvOp;
                    final Translog.Index o2 = (Translog.Index) newOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && Objects.equals(o1.source(), o2.source())
                        && Objects.equals(o1.routing(), o2.routing())
                        && o1.primaryTerm() == o2.primaryTerm()
                        && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else if (newOp instanceof Translog.Delete && prvOp instanceof Translog.Delete) {
                    final Translog.Delete o1 = (Translog.Delete) newOp;
                    final Translog.Delete o2 = (Translog.Delete) prvOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && o1.primaryTerm() == o2.primaryTerm()
                        && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else {
                    sameOp = false;
                }
                if (sameOp == false) {
                    throw new AssertionError(
                        "seqNo ["
                            + seqNo
                            + "] was processed twice in generation ["
                            + generation
                            + "], with different data. "
                            + "prvOp ["
                            + prvOp
                            + "], newOp ["
                            + newOp
                            + "]",
                        previous.v2()
                    );
                }
            }
        } else {
            seenSequenceNumbers.put(
                seqNo,
                new Tuple<>(new BytesArray(data.toBytesRef(), true), new RuntimeException("stack capture previous op"))
            );
        }
        return true;
    }

    synchronized boolean assertNoSeqAbove(long belowTerm, long aboveSeqNo) {
        seenSequenceNumbers.entrySet().stream().filter(e -> e.getKey().longValue() > aboveSeqNo).forEach(e -> {
            final Translog.Operation op;
            try {
                op = Translog.readOperation(new BufferedChecksumStreamInput(e.getValue().v1().streamInput(), "assertion"));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            long seqNo = op.seqNo();
            long primaryTerm = op.primaryTerm();
            if (primaryTerm < belowTerm) {
                throw new AssertionError(
                    "current should not have any operations with seq#:primaryTerm ["
                        + seqNo
                        + ":"
                        + primaryTerm
                        + "] > "
                        + aboveSeqNo
                        + ":"
                        + belowTerm
                );
            }
        });
        return true;
    }

    /**
     * write all buffered ops to disk and fsync file.
     * <p>
     * Note: any exception during the sync process will be interpreted as a tragic exception and the writer will be closed before
     * raising the exception.
     * @return  <code>true</code> if this call caused an actual sync operation
     */
    public boolean sync() throws IOException {
        return syncUpTo(Long.MAX_VALUE);
    }

    /**
     * Returns <code>true</code> if there are buffered operations that have not been flushed and fsynced to disk or if the latest global
     * checkpoint has not yet been fsynced
     */
    public boolean syncNeeded() {
        return totalOffset != lastSyncedCheckpoint.offset
            || globalCheckpointSupplier.getAsLong() != lastSyncedCheckpoint.globalCheckpoint
            || minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    synchronized Checkpoint getCheckpoint() {
        return new Checkpoint(
            totalOffset,
            operationCounter,
            generation,
            minSeqNo,
            maxSeqNo,
            globalCheckpointSupplier.getAsLong(),
            minTranslogGenerationSupplier.getAsLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO
        );
    }

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }

    /**
     * Closes this writer and transfers its underlying file channel to a new immutable {@link TranslogReader}
     * @return a new {@link TranslogReader}
     * @throws IOException if any of the file operations resulted in an I/O exception
     */
    public TranslogReader closeIntoReader() throws IOException {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        // After the sync lock we acquire the write lock to avoid deadlocks with threads writing where
        // the write lock is acquired first followed by synchronize(this).
        //
        // Note: While this is not strictly needed as this method is called while blocking all ops on the translog,
        // we do this to for correctness and preventing future issues.
        synchronized (syncLock) {
            try (ReleasableLock toClose = writeLock.acquire()) {
                synchronized (this) {
                    try {
                        sync(); // sync before we close..
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    // If we reached this point, all of the buffered ops should have been flushed successfully.
                    assert buffer == null;
                    assert checkChannelPositionWhileHandlingException(totalOffset);
                    assert totalOffset == lastSyncedCheckpoint.offset;
                    if (closed.compareAndSet(false, true)) {
                        try {
                            checkpointChannel.close();
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                        return new TranslogReader(
                            getLastSyncedCheckpoint(),
                            channel,
                            path,
                            header,
                            (translogCheckedContainer != null) ? translogCheckedContainer.getChecksum() : null
                        );
                    } else {
                        throw new AlreadyClosedException(
                            "translog [" + getGeneration() + "] is already closed (path [" + path + "]",
                            tragedy.get()
                        );
                    }
                }
            }
        }
    }

    @Override
    TranslogSnapshot newSnapshot() {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        // After the sync lock we acquire the write lock to avoid deadlocks with threads writing where
        // the write lock is acquired first followed by synchronize(this).
        synchronized (syncLock) {
            try (ReleasableLock toClose = writeLock.acquire()) {
                synchronized (this) {
                    ensureOpen();
                    try {
                        sync();
                    } catch (IOException e) {
                        throw new TranslogException(shardId, "exception while syncing before creating a snapshot", e);
                    }
                    // If we reached this point, all of the buffered ops should have been flushed successfully.
                    assert buffer == null;
                    assert checkChannelPositionWhileHandlingException(totalOffset);
                    assert totalOffset == lastSyncedCheckpoint.offset;
                    return super.newSnapshot();
                }
            }
        }
    }

    private long getWrittenOffset() throws IOException {
        return channel.position();
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     */
    final boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    // double checked locking - we don't want to fsync unless we have to and now that we have
                    // the lock we should check again since if this code is busy we might have fsynced enough already
                    final Checkpoint checkpointToSync;
                    final List<Long> flushedSequenceNumbers;
                    final ReleasableBytesReference toWrite;
                    try (ReleasableLock toClose = writeLock.acquire()) {
                        synchronized (this) {
                            ensureOpen();
                            checkpointToSync = getCheckpoint();
                            toWrite = pollOpsToWrite();
                            flushedSequenceNumbers = nonFsyncedSequenceNumbers;
                            nonFsyncedSequenceNumbers = new ArrayList<>(64);
                        }

                        try {
                            // Write ops will release operations.
                            writeAndReleaseOps(toWrite);
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                    }
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.
                    try {
                        if (!Boolean.TRUE.equals(remoteTranslogEnabled)) {
                            channel.force(false);
                        }
                        writeCheckpoint(checkpointChannel, checkpointPath, checkpointToSync, remoteTranslogEnabled);
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    flushedSequenceNumbers.forEach(persistedSequenceNumberConsumer::accept);
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset : "illegal state: "
                        + lastSyncedCheckpoint.offset
                        + " <= "
                        + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync; // write protected by syncLock
                    return true;
                }
            }
        }
        return false;
    }

    private void writeBufferedOps(long offset, boolean blockOnExistingWriter) throws IOException {
        try (ReleasableLock locked = blockOnExistingWriter ? writeLock.acquire() : writeLock.tryAcquire()) {
            try {
                if (locked != null && offset > getWrittenOffset()) {
                    writeAndReleaseOps(pollOpsToWrite());
                }
            } catch (Exception e) {
                closeWithTragicEvent(e);
                throw e;
            }
        }
    }

    private synchronized ReleasableBytesReference pollOpsToWrite() {
        ensureOpen();
        if (this.buffer != null) {
            ReleasableBytesStreamOutput toWrite = this.buffer;
            this.buffer = null;
            this.bufferedBytes = 0;
            return new ReleasableBytesReference(toWrite.bytes(), toWrite);
        } else {
            return ReleasableBytesReference.wrap(BytesArray.EMPTY);
        }
    }

    private void writeAndReleaseOps(ReleasableBytesReference toWrite) throws IOException {
        try (ReleasableBytesReference toClose = toWrite) {
            assert writeLock.isHeldByCurrentThread();
            ByteBuffer ioBuffer = DiskIoBufferPool.getIoBuffer();

            BytesRefIterator iterator = toWrite.iterator();
            BytesRef current;
            while ((current = iterator.next()) != null) {
                int currentBytesConsumed = 0;
                while (currentBytesConsumed != current.length) {
                    int nBytesToWrite = Math.min(current.length - currentBytesConsumed, ioBuffer.remaining());
                    ioBuffer.put(current.bytes, current.offset + currentBytesConsumed, nBytesToWrite);
                    if (translogCheckedContainer != null) {
                        translogCheckedContainer.updateFromBytes(current.bytes, current.offset + currentBytesConsumed, nBytesToWrite);
                    }
                    currentBytesConsumed += nBytesToWrite;
                    if (ioBuffer.hasRemaining() == false) {
                        ioBuffer.flip();
                        writeToFile(ioBuffer);
                        ioBuffer.clear();
                    }
                }
            }
            ioBuffer.flip();
            writeToFile(ioBuffer);
        }
    }

    @SuppressForbidden(reason = "Channel#write")
    private void writeToFile(ByteBuffer ioBuffer) throws IOException {
        while (ioBuffer.remaining() > 0) {
            channel.write(ioBuffer);
        }
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try {
            if (position + targetBuffer.remaining() > getWrittenOffset()) {
                // we only flush here if it's really really needed - try to minimize the impact of the read operation
                // in some cases ie. a tragic event we might still be able to read the relevant value
                // which is not really important in production but some test can make most strict assumptions
                // if we don't fail in this call unless absolutely necessary.
                writeBufferedOps(position + targetBuffer.remaining(), true);
            }
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    private static void writeCheckpoint(
        final FileChannel fileChannel,
        final Path checkpointFile,
        final Checkpoint checkpoint,
        final Boolean remoteTranslogEnabled
    ) throws IOException {
        Checkpoint.write(fileChannel, checkpointFile, checkpoint, !Boolean.TRUE.equals(remoteTranslogEnabled));
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     */
    Checkpoint getLastSyncedCheckpoint() {
        return lastSyncedCheckpoint;
    }

    protected final void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy.get());
        }
    }

    private boolean checkChannelPositionWhileHandlingException(long expectedOffset) {
        try {
            return expectedOffset == channel.position();
        } catch (IOException e) {
            return true;
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            synchronized (this) {
                Releasables.closeWhileHandlingException(buffer);
                buffer = null;
                bufferedBytes = 0;
            }
            IOUtils.close(checkpointChannel, channel);
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }
}
