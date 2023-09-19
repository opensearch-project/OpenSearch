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
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Index input that calculates checksum as data is read from the input.
 * <p>
 * This class supports random access (it is possible to seek backward and forward) in order to accommodate retry
 * mechanism that is used in some repository plugins (S3 for example). However, the checksum is only calculated on
 * the first read. All consecutive reads of the same data are not used to calculate the checksum.
 *
 * @opensearch.internal
 */
public class VerifyingIndexInput extends ChecksumIndexInput {
    private final IndexInput input;
    private final Checksum digest;
    private final long checksumPosition;
    private final byte[] checksum = new byte[8];
    private long verifiedPosition = 0;

    VerifyingIndexInput(IndexInput input) {
        this(input, new BufferedChecksum(new CRC32()));
    }

    VerifyingIndexInput(IndexInput input, Checksum digest) {
        super("VerifyingIndexInput(" + input + ")");
        this.input = input;
        this.digest = digest;
        checksumPosition = input.length() - 8;
    }

    @Override
    public byte readByte() throws IOException {
        long pos = input.getFilePointer();
        final byte b = input.readByte();
        pos++;
        if (pos > verifiedPosition) {
            if (pos <= checksumPosition) {
                digest.update(b);
            } else {
                checksum[(int) (pos - checksumPosition - 1)] = b;
            }
            verifiedPosition = pos;
        }
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        long pos = input.getFilePointer();
        input.readBytes(b, offset, len);
        if (pos + len > verifiedPosition) {
            // Conversion to int is safe here because (verifiedPosition - pos) can be at most len, which is integer
            int alreadyVerified = (int) Math.max(0, verifiedPosition - pos);
            if (pos < checksumPosition) {
                if (pos + len < checksumPosition) {
                    digest.update(b, offset + alreadyVerified, len - alreadyVerified);
                } else {
                    int checksumOffset = (int) (checksumPosition - pos);
                    if (checksumOffset - alreadyVerified > 0) {
                        digest.update(b, offset + alreadyVerified, checksumOffset - alreadyVerified);
                    }
                    System.arraycopy(b, offset + checksumOffset, checksum, 0, len - checksumOffset);
                }
            } else {
                // Conversion to int is safe here because checksumPosition is (file length - 8) so
                // (pos - checksumPosition) cannot be bigger than 8 unless we are reading after the end of file
                assert pos - checksumPosition < 8;
                System.arraycopy(b, offset, checksum, (int) (pos - checksumPosition), len);
            }
            verifiedPosition = pos + len;
        }
    }

    @Override
    public long getChecksum() {
        return digest.getValue();
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < verifiedPosition) {
            // going within verified region - just seek there
            input.seek(pos);
        } else {
            if (verifiedPosition > getFilePointer()) {
                // portion of the skip region is verified and portion is not
                // skipping the verified portion
                input.seek(verifiedPosition);
                // and checking unverified
                super.seek(pos);
            } else {
                super.seek(pos);
            }
        }
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public long getFilePointer() {
        return input.getFilePointer();
    }

    @Override
    public long length() {
        return input.length();
    }

    @Override
    public IndexInput clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException();
    }

    public long getStoredChecksum() {
        try {
            return CodecUtil.readBELong(new ByteArrayDataInput(checksum));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long verify() throws CorruptIndexException, IOException {
        long storedChecksum = getStoredChecksum();
        if (getChecksum() == storedChecksum) {
            return storedChecksum;
        }
        throw new CorruptIndexException(
            "verification failed : calculated=" + Store.digestToString(getChecksum()) + " stored=" + Store.digestToString(storedChecksum),
            this
        );
    }
}
