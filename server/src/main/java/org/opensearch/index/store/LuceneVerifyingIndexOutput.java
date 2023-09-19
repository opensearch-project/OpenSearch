/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;

import java.io.IOException;

/**
 * Class to verify the lucene index output
 *
 * @opensearch.internal
 */
public class LuceneVerifyingIndexOutput extends VerifyingIndexOutput {

    private final StoreFileMetadata metadata;
    private long writtenBytes;
    private final long checksumPosition;
    private String actualChecksum;
    private final byte[] footerChecksum = new byte[8]; // this holds the actual footer checksum data written by to this output

    public LuceneVerifyingIndexOutput(StoreFileMetadata metadata, IndexOutput out) {
        super(out);
        this.metadata = metadata;
        checksumPosition = metadata.length() - 8; // the last 8 bytes are the checksum - we store it in footerChecksum
    }

    @Override
    public void verify() throws IOException {
        String footerDigest = null;
        if (metadata.checksum().equals(actualChecksum) && writtenBytes == metadata.length()) {
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("checksum", this.footerChecksum);
            footerDigest = Store.digestToString(CodecUtil.readBELong(indexInput));
            if (metadata.checksum().equals(footerDigest)) {
                return;
            }
        }
        throw new CorruptIndexException(
            "verification failed (hardware problem?) : expected="
                + metadata.checksum()
                + " actual="
                + actualChecksum
                + " footer="
                + footerDigest
                + " writtenLength="
                + writtenBytes
                + " expectedLength="
                + metadata.length()
                + " (resource="
                + metadata
                + ")",
            "VerifyingIndexOutput(" + metadata.name() + ")"
        );
    }

    @Override
    public void writeByte(byte b) throws IOException {
        final long writtenBytes = this.writtenBytes++;
        if (writtenBytes >= checksumPosition) { // we are writing parts of the checksum....
            if (writtenBytes == checksumPosition) {
                readAndCompareChecksum();
            }
            final int index = Math.toIntExact(writtenBytes - checksumPosition);
            if (index < footerChecksum.length) {
                footerChecksum[index] = b;
                if (index == footerChecksum.length - 1) {
                    verify(); // we have recorded the entire checksum
                }
            } else {
                verify(); // fail if we write more than expected
                throw new AssertionError("write past EOF expected length: " + metadata.length() + " writtenBytes: " + writtenBytes);
            }
        }
        out.writeByte(b);
    }

    private void readAndCompareChecksum() throws IOException {
        actualChecksum = Store.digestToString(getChecksum());
        if (!metadata.checksum().equals(actualChecksum)) {
            throw new CorruptIndexException(
                "checksum failed (hardware problem?) : expected="
                    + metadata.checksum()
                    + " actual="
                    + actualChecksum
                    + " (resource="
                    + metadata.toString()
                    + ")",
                "VerifyingIndexOutput(" + metadata.name() + ")"
            );
        }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        if (writtenBytes + length > checksumPosition) {
            for (int i = 0; i < length; i++) { // don't optimze writing the last block of bytes
                writeByte(b[offset + i]);
            }
        } else {
            out.writeBytes(b, offset, length);
            writtenBytes += length;
        }
    }
}
