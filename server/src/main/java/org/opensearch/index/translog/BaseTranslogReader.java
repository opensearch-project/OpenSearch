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

import org.opensearch.core.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.core.common.io.stream.ByteBufferStreamInput;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A base class for all classes that allows reading ops from translog files
 *
 * @opensearch.internal
 */
public abstract class BaseTranslogReader implements Comparable<BaseTranslogReader> {

    protected final long generation;
    protected final FileChannel channel;
    protected final Path path;
    protected final TranslogHeader header;

    public BaseTranslogReader(long generation, FileChannel channel, Path path, TranslogHeader header) {
        assert Translog.parseIdFromFileName(path) == generation : "generation mismatch. Path: "
            + Translog.parseIdFromFileName(path)
            + " but generation: "
            + generation;

        this.generation = generation;
        this.path = path;
        this.channel = channel;
        this.header = header;
    }

    public long getGeneration() {
        return this.generation;
    }

    public abstract long sizeInBytes();

    public abstract int totalOperations();

    abstract Checkpoint getCheckpoint();

    public final long getFirstOperationOffset() {
        return header.sizeInBytes();
    }

    /**
     * Returns the primary term associated with this translog reader.
     */
    public final long getPrimaryTerm() {
        return header.getPrimaryTerm();
    }

    /** read the size of the op (i.e., number of bytes, including the op size) written at the given position */
    protected final int readSize(ByteBuffer reusableBuffer, long position) throws IOException {
        // read op size from disk
        assert reusableBuffer.capacity() >= 4 : "reusable buffer must have capacity >=4 when reading opSize. got ["
            + reusableBuffer.capacity()
            + "]";
        reusableBuffer.clear();
        reusableBuffer.limit(4);
        readBytes(reusableBuffer, position);
        reusableBuffer.flip();
        // Add an extra 4 to account for the operation size integer itself
        final int size = reusableBuffer.getInt() + 4;
        final long maxSize = sizeInBytes() - position;
        if (size < 0 || size > maxSize) {
            throw new TranslogCorruptedException(
                path.toString(),
                "operation size is corrupted must be [0.." + maxSize + "] but was: " + size
            );
        }
        return size;
    }

    TranslogSnapshot newSnapshot() {
        return new TranslogSnapshot(this, sizeInBytes());
    }

    /**
     * reads an operation at the given position and returns it. The buffer length is equal to the number
     * of bytes reads.
     */
    protected final BufferedChecksumStreamInput checksummedStream(
        ByteBuffer reusableBuffer,
        long position,
        int opSize,
        BufferedChecksumStreamInput reuse
    ) throws IOException {
        final ByteBuffer buffer;
        if (reusableBuffer.capacity() >= opSize) {
            buffer = reusableBuffer;
        } else {
            buffer = ByteBuffer.allocate(opSize);
        }
        buffer.clear();
        buffer.limit(opSize);
        readBytes(buffer, position);
        buffer.flip();
        return new BufferedChecksumStreamInput(new ByteBufferStreamInput(buffer), path.toString(), reuse);
    }

    protected Translog.Operation read(BufferedChecksumStreamInput inStream) throws IOException {
        final Translog.Operation op = Translog.readOperation(inStream);
        if (op.primaryTerm() > getPrimaryTerm() && getPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            throw new TranslogCorruptedException(
                path.toString(),
                "operation's term is newer than translog header term; "
                    + "operation term["
                    + op.primaryTerm()
                    + "], translog header term ["
                    + getPrimaryTerm()
                    + "]"
            );
        }
        return op;
    }

    /**
     * reads bytes at position into the given buffer, filling it.
     */
    protected abstract void readBytes(ByteBuffer buffer, long position) throws IOException;

    @Override
    public String toString() {
        return "translog [" + generation + "][" + path + "]";
    }

    @Override
    public int compareTo(BaseTranslogReader o) {
        return Long.compare(getGeneration(), o.getGeneration());
    }

    public Path path() {
        return path;
    }

    public long getLastModifiedTime() throws IOException {
        return Files.getLastModifiedTime(path).toMillis();
    }

    /**
     * Reads a single operation from the given location.
     */
    Translog.Operation read(Translog.Location location) throws IOException {
        assert location.generation == this.generation : "generation mismatch expected: " + generation + " got: " + location.generation;
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        return read(checksummedStream(buffer, location.translogLocation, location.size, null));
    }
}
