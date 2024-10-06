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
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.io.Channels;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.index.translog.Translog.getCommitCheckpointFileName;

/**
 * an immutable translog filereader
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TranslogReader extends BaseTranslogReader implements Closeable {
    protected final long length;
    private final int totalOperations;
    private final Checkpoint checkpoint;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    @Nullable
    private final Long translogChecksum;

    // fullTranslogChecksum is the checksum for translog which includes header, content, and footer.
    @Nullable
    private final Long fullTranslogChecksum;
    @Nullable
    private final Long checkpointChecksum;
    private final Boolean hasFooter;

    /**
     * Create a translog writer against the specified translog file channel.
     *
     * @param checkpoint the translog checkpoint
     * @param channel    the translog file channel to open a translog reader against
     * @param path       the path to the translog
     * @param header     the header of the translog file
     */
    TranslogReader(
        final Checkpoint checkpoint,
        final FileChannel channel,
        final Path path,
        final TranslogHeader header,
        final Long translogChecksum,
        final Long fullTranslogChecksum,
        final Boolean hasFooter
    ) throws IOException {
        super(checkpoint.generation, channel, path, header);
        this.length = checkpoint.offset;
        this.totalOperations = checkpoint.numOps;
        this.checkpoint = checkpoint;
        this.translogChecksum = translogChecksum;
        this.fullTranslogChecksum = fullTranslogChecksum;
        this.checkpointChecksum = (translogChecksum != null) ? calculateCheckpointChecksum(checkpoint, path) : null;
        this.hasFooter = hasFooter;
    }

    private static Long calculateCheckpointChecksum(Checkpoint checkpoint, Path path) throws IOException {
        TranslogCheckedContainer checkpointCheckedContainer = new TranslogCheckedContainer(
            Checkpoint.createCheckpointBytes(path.getParent().resolve(Translog.CHECKPOINT_FILE_NAME), checkpoint)
        );
        return checkpointCheckedContainer.getChecksum();
    }

    public Long getTranslogChecksum() {
        return translogChecksum;
    }

    /**
     * getFullTranslogChecksum returns the complete checksum of the translog which includes
     * header, content and footer.
     * */
    public Long getFullTranslogChecksum() {
        return fullTranslogChecksum;
    }

    public Long getCheckpointChecksum() {
        return checkpointChecksum;
    }

    /**
     * Given a file channel, opens a {@link TranslogReader}, taking care of checking and validating the file header.
     *
     * @param channel the translog file channel
     * @param path the path to the translog
     * @param checkpoint the translog checkpoint
     * @param translogUUID the tranlog UUID
     * @return a new TranslogReader
     * @throws IOException if any of the file operations resulted in an I/O exception
     */
    public static TranslogReader open(final FileChannel channel, final Path path, final Checkpoint checkpoint, final String translogUUID)
        throws IOException {
        final TranslogHeader header = TranslogHeader.read(translogUUID, path, channel);

        // When we open a reader to Translog from a path, we want to fetch the checksum
        // as that would be needed later on while creating the metadata map for
        // generation to checksum.
        Long translogChecksum = null;
        try {
            translogChecksum = TranslogFooter.readChecksum(path);
        } catch (IOException ignored) {}

        boolean hasFooter = translogChecksum != null;

        return new TranslogReader(checkpoint, channel, path, header, translogChecksum, null, hasFooter);
    }

    /**
     * Closes current reader and creates new one with new checkoint and same file channel
     */
    TranslogReader closeIntoTrimmedReader(long aboveSeqNo, ChannelFactory channelFactory) throws IOException {
        if (closed.compareAndSet(false, true)) {
            Closeable toCloseOnFailure = channel;
            final TranslogReader newReader;
            try {
                if (aboveSeqNo < checkpoint.trimmedAboveSeqNo
                    || aboveSeqNo < checkpoint.maxSeqNo && checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    final Path checkpointFile = path.getParent().resolve(getCommitCheckpointFileName(checkpoint.generation));
                    final Checkpoint newCheckpoint = new Checkpoint(
                        checkpoint.offset,
                        checkpoint.numOps,
                        checkpoint.generation,
                        checkpoint.minSeqNo,
                        checkpoint.maxSeqNo,
                        checkpoint.globalCheckpoint,
                        checkpoint.minTranslogGeneration,
                        aboveSeqNo
                    );
                    Checkpoint.write(channelFactory, checkpointFile, newCheckpoint, StandardOpenOption.WRITE);

                    IOUtils.fsync(checkpointFile.getParent(), true);

                    newReader = new TranslogReader(newCheckpoint, channel, path, header, translogChecksum, fullTranslogChecksum, hasFooter);
                } else {
                    newReader = new TranslogReader(checkpoint, channel, path, header, translogChecksum, fullTranslogChecksum, hasFooter);
                }
                toCloseOnFailure = null;
                return newReader;
            } finally {
                IOUtils.close(toCloseOnFailure);
            }
        } else {
            throw new AlreadyClosedException(toString() + " is already closed");
        }
    }

    public long sizeInBytes() {
        return length;
    }

    public int totalOperations() {
        return totalOperations;
    }

    @Override
    final public Checkpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        if (hasFooter && header.getTranslogHeaderVersion() == TranslogHeader.VERSION_WITH_FOOTER) {
            // Ensure that the read request does not overlap with footer.
            long translogLengthWithoutFooter = length - TranslogFooter.footerLength();
            if (position >= translogLengthWithoutFooter && position < length) {
                throw new EOFException(
                    "read requested past last ops into footer. pos [" + position + "] end: [" + translogLengthWithoutFooter + "]"
                );
            }
            // If we are trying to read beyond the last Ops, we need to return EOF error.
            long lastPositionToRead = position + buffer.limit();
            if (lastPositionToRead > translogLengthWithoutFooter) {
                throw new EOFException(
                    "trying to read past last ops into footer. pos [" + lastPositionToRead + "] end: [" + translogLengthWithoutFooter + "]"
                );
            }
        }

        if (position >= length) {
            throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + length + "]");
        }
        if (position < getFirstOperationOffset()) {
            throw new IOException(
                "read requested before position of first ops. pos [" + position + "] first op on: [" + getFirstOperationOffset() + "]"
            );
        }
        Channels.readFromFileChannelWithEofException(channel, position, buffer);
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            channel.close();
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException(toString() + " is already closed");
        }
    }

    public long getMinSeqNo() {
        return checkpoint.minSeqNo;
    }

    public long getMaxSeqNo() {
        return checkpoint.maxSeqNo;
    }
}
