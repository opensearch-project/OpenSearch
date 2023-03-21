/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteIndexOutput;
import org.opensearch.index.store.lockmanager.RemoteLockDirectory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class for output to a file in a {@link RemoteLockDirectory}. Used for all output operations to the remote store.
 * @see RemoteLockDirectory
 *
 * @opensearch.internal
 */
public class RemoteLockIndexOutput extends RemoteIndexOutput {
    private final BlobContainer blobContainer;
    private final BytesStreamOutput out;
    private final OutputStreamIndexOutput indexOutputBuffer;
    private static final int BUFFER_SIZE = 4096;
    public RemoteLockIndexOutput(String name, BlobContainer blobContainer) {
        super(name, blobContainer);
        this.blobContainer = blobContainer;
        out = new BytesStreamOutput();
        indexOutputBuffer = new OutputStreamIndexOutput(
            name,
            name,
            out,
            BUFFER_SIZE
        );
    }

    /**
     * when we trigger close() to close the stream, we will first flush the buffer to output stream and then write all
     * data to blob container and close the output stream.
     *
     */
    @Override
    public void close() throws IOException {
        indexOutputBuffer.close();
        try (InputStream stream = out.bytes().streamInput()) {
            blobContainer.writeBlob(getName(), stream, out.bytes().length(), false);
        }
        out.close();
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
     * But the checksum is important to verify integrity of the data and that means implementing this method will
     * be required for the segment upload as well.
     *
     */
    @Override
    public long getChecksum() throws IOException {
        return indexOutputBuffer.getChecksum();
    }
}
