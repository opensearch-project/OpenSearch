/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class for input from a file in a {@link RemoteDirectory}. Used for all input operations from the remote store.
 * Currently, only methods from {@link IndexInput} that are required for reading a file from remote store are
 * implemented. Remaining methods will be implemented as we open up remote store for other use cases like replication,
 * peer recovery etc.
 * ToDo: Extend ChecksumIndexInput
 * @see RemoteDirectory
 *
 * @opensearch.internal
 */
public class RemoteIndexInput extends IndexInput {

    private final InputStream inputStream;
    private final long size;

    public RemoteIndexInput(String name, InputStream inputStream, long size) {
        super(name);
        this.inputStream = inputStream;
        this.size = size;
    }

    @Override
    public byte readByte() throws IOException {
        byte[] buffer = new byte[1];
        inputStream.read(buffer);
        return buffer[0];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        int bytesRead = inputStream.read(b, offset, len);
        while (bytesRead > 0 && bytesRead < len) {
            len -= bytesRead;
            offset += bytesRead;
            bytesRead = inputStream.read(b, offset, len);
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public long length() {
        return size;
    }

    @Override
    public void seek(long pos) throws IOException {
        inputStream.skip(pos);
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexInput unmodified.
     * This method is not implemented as it is not used for the file transfer to/from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public long getFilePointer() {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the RemoteIndexInput unmodified.
     * This method is not implemented as it is not used for the file transfer to/from the remote store.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException();
    }
}
