/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;

import java.io.IOException;
import java.io.InputStream;

public class OffsetRangeIndexInputStream extends InputStreamIndexInput {

    private final IndexInput indexInput;
    private final String fileName;

    // This is the maximum position till stream is to be read. If read methods exceed maxPos then bytes are read
    // till maxPos. If no byte is left after maxPos, then -1 is returned from read methods.
    private final long maxLen;
    // Position in stream from which read will start.
    private long curLen;

    public OffsetRangeIndexInputStream(IndexInput indexInput, String fileName, long maxLen, long position) throws IOException {
        super(indexInput, maxLen);
        this.indexInput = indexInput;
        this.indexInput.seek(position);
        this.fileName = fileName;
        this.maxLen = maxLen;
    }

    @Override
    public int read() throws IOException {
        if (hasBytesConsumed()) return -1;
        curLen++;
        return super.read();
    }

    @Override
    public int read(byte []b, int off, int len) throws IOException {
        if (hasBytesConsumed()) return -1;
        long inputLen = limitLength(len);
        curLen += inputLen;
        return super.read(b, off, (int)inputLen);
    }

    private long limitLength(int len) {
        if (len < 0) return 0;
        long lengthAfterRead = curLen + len;
        return lengthAfterRead < maxLen ? len : maxLen - curLen;
    }

    private boolean hasBytesConsumed() {
        return curLen >= maxLen;
    }
}
