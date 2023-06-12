/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;

import java.io.IOException;

/**
 * OffsetRangeIndexInputStream extends InputStream to read from a specified offset using IndexInput
 *
 * @opensearch.internal
 */
public class OffsetRangeIndexInputStream extends OffsetRangeInputStream {

    private final InputStreamIndexInput inputStreamIndexInput;
    private final IndexInput indexInput;

    /**
     * Construct a new OffsetRangeIndexInputStream object
     *
     * @param indexInput IndexInput opened on the file to read from
     * @param size The maximum length to read from specified <code>position</code>
     * @param position Position from where read needs to start
     * @throws IOException When <code>IndexInput#seek</code> operation fails
     */
    public OffsetRangeIndexInputStream(IndexInput indexInput, long size, long position) throws IOException {
        indexInput.seek(position);
        this.indexInput = indexInput;
        this.inputStreamIndexInput = new InputStreamIndexInput(indexInput, size);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inputStreamIndexInput.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
        return inputStreamIndexInput.read();
    }

    @Override
    public boolean markSupported() {
        return inputStreamIndexInput.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
        inputStreamIndexInput.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        inputStreamIndexInput.reset();
    }

    @Override
    public long getFilePointer() throws IOException {
        return indexInput.getFilePointer();
    }

    @Override
    public void close() throws IOException {
        inputStreamIndexInput.close();
        indexInput.close();
    }
}
