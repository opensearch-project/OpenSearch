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

import org.opensearch.common.io.Channels;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A snapshot of translog operations
 *
 * @opensearch.internal
 */
final class TranslogSnapshot extends BaseTranslogReader {

    private final int totalOperations;
    private final Checkpoint checkpoint;
    protected final long length;

    private final ByteBuffer reusableBuffer;
    private long position;
    private int skippedOperations;
    private int readOperations;
    private BufferedChecksumStreamInput reuse;

    /**
     * Create a snapshot of translog file channel.
     */
    TranslogSnapshot(final BaseTranslogReader reader, final long length) {
        super(reader.generation, reader.channel, reader.path, reader.header);
        this.length = length;
        this.totalOperations = reader.totalOperations();
        this.checkpoint = reader.getCheckpoint();
        this.reusableBuffer = ByteBuffer.allocate(1024);
        this.readOperations = 0;
        this.position = reader.getFirstOperationOffset();
        this.reuse = null;
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    int skippedOperations() {
        return skippedOperations;
    }

    @Override
    Checkpoint getCheckpoint() {
        return checkpoint;
    }

    public Translog.Operation next() throws IOException {
        while (readOperations < totalOperations) {
            final Translog.Operation operation = readOperation();
            if (operation.seqNo() <= checkpoint.trimmedAboveSeqNo || checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                return operation;
            }
            skippedOperations++;
        }
        return null;
    }

    private Translog.Operation readOperation() throws IOException {
        final int opSize = readSize(reusableBuffer, position);
        reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
        Translog.Operation op = read(reuse);
        position += opSize;
        readOperations++;
        return op;
    }

    public long sizeInBytes() {
        return length;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try {
            if (position >= length) {
                throw new EOFException(
                    "read requested past EOF. pos ["
                        + position
                        + "] end: ["
                        + length
                        + "], generation: ["
                        + getGeneration()
                        + "], path: ["
                        + path
                        + "]"
                );
            }
            if (position < getFirstOperationOffset()) {
                throw new IOException(
                    "read requested before position of first ops. pos ["
                        + position
                        + "] first op on: ["
                        + getFirstOperationOffset()
                        + "], generation: ["
                        + getGeneration()
                        + "], path: ["
                        + path
                        + "]"
                );
            }
            Channels.readFromFileChannelWithEofException(channel, position, buffer);
        } catch (EOFException e) {
            throw new TranslogCorruptedException(path.toString(), "translog truncated", e);
        }
    }

    @Override
    public String toString() {
        return "TranslogSnapshot{"
            + "readOperations="
            + readOperations
            + ", position="
            + position
            + ", estimateTotalOperations="
            + totalOperations
            + ", length="
            + length
            + ", generation="
            + generation
            + ", reusableBuffer="
            + reusableBuffer
            + '}';
    }
}
