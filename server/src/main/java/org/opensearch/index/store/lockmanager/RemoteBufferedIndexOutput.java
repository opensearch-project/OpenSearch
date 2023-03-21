/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.index.store.RemoteIndexOutput;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;

/**
 * Class for output to a file in a {@link RemoteBufferedOutputDirectory}. Used for all output operations to the remote store.
 * @see RemoteBufferedOutputDirectory
 *
 * @opensearch.internal
 */
public class RemoteBufferedIndexOutput extends RemoteIndexOutput {
    private final BlobContainer blobContainer;
    private final BytesStreamOutput out;
    private final OutputStreamIndexOutput indexOutputBuffer;
    // visible for testing
    static final int BUFFER_SIZE = 4096;

    public RemoteBufferedIndexOutput(String name, BlobContainer blobContainer, int bufferSize) {
        super(name, blobContainer);
        this.blobContainer = blobContainer;
        out = new BytesStreamOutput();
        indexOutputBuffer = new OutputStreamIndexOutput(
            name,
            name,
            out,
            bufferSize
        );
    }
    public RemoteBufferedIndexOutput(String name, BlobContainer blobContainer) {
        this(name, blobContainer, BUFFER_SIZE);
    }

    // Visible for testing
    RemoteBufferedIndexOutput(String name, BlobContainer blobContainer,BytesStreamOutput out,
                              OutputStreamIndexOutput indexOutputBuffer) {
        super(name, blobContainer);
        this.blobContainer = blobContainer;
        this.out = out;
        this.indexOutputBuffer = indexOutputBuffer;
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException {
        indexOutputBuffer.copyBytes(input, numBytes);
    }

    /**
     * when we trigger close() to close the stream, we will first flush the buffer to output stream and then write all
     * data to blob container and close the output stream.
     *
     */
    @Override
    public void close() throws IOException {
        indexOutputBuffer.close();
        try (final BytesStreamOutput outStream = out; InputStream stream = out.bytes().streamInput()) {
            blobContainer.writeBlob(getName(), stream, out.bytes().length(), false);
        }
    }

    /**
     * This method will write Bytes to the stream we are maintaining.
     *
     */
    @Override
    public void writeByte(byte b) throws IOException {
        indexOutputBuffer.writeByte(b);
    }

    /**
     * This method will write a byte array to the stream we are maintaining.
     *
     */
    @Override
    public void writeBytes(byte[] byteArray, int offset, int length) throws IOException {
        indexOutputBuffer.writeBytes(byteArray,offset, length);
    }

    /**
     * This method will return the file pointer to the current position in the stream.
     *
     */
    @Override
    public long getFilePointer() {
        return indexOutputBuffer.getFilePointer();
    }

    /**
     * This method will return checksum
     *
     */
    @Override
    public long getChecksum() throws IOException {
        return indexOutputBuffer.getChecksum();
    }
}
