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

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.Checksum;

/**
 * Abstract index input that digests data as it is read, so that what was read can be verified.
 * Subclasses override {@link #verify()} to compare the digest against whatever their data format treats as the
 * expected value: a Lucene codec footer, or a checksum recorded in {@link StoreFileMetadata}.
 * <p>
 * This class supports random access (it is possible to seek backward and forward) in order to accommodate retry
 * mechanism that is used in some repository plugins (S3 for example). However, the checksum is only calculated on
 * the first read. All consecutive reads of the same data are not used to calculate the checksum.
 *
 * @opensearch.experimental
 */
// do NOT optimize this class for performance
@ExperimentalApi
public abstract class VerifyingIndexInput extends ChecksumIndexInput {
    /** Width of the 64-bit checksum Lucene stores at the tail of a codec footer. */
    protected static final int STORED_CHECKSUM_LENGTH = 8;

    private final IndexInput input;
    private final Checksum digest;
    private final long checksumPosition;
    private final byte[] checksum = new byte[STORED_CHECKSUM_LENGTH];
    private long verifiedPosition = 0;

    /**
     * @param checksumPosition offset at which the trailing stored checksum begins. Bytes from this offset onward
     *                         are captured for {@link #getStoredChecksum()} instead of being digested. Pass
     *                         {@code input.length()} for formats that carry no trailing checksum, so that every
     *                         byte of the file feeds the digest.
     */
    VerifyingIndexInput(IndexInput input, Checksum digest, long checksumPosition) {
        super("VerifyingIndexInput(" + input + ")");
        this.input = input;
        this.digest = digest;
        this.checksumPosition = checksumPosition;
    }

    /**
     * Verifies that what was read matches what this data format expects, and returns the checksum that was
     * compared. Call this only after the input has been read in full.
     */
    public abstract long verify() throws CorruptIndexException, IOException;

    /** Number of bytes of this input that have fed the digest. */
    protected final long verifiedPosition() {
        return verifiedPosition;
    }

    /**
     * The trailing bytes captured from the offset given as {@code checksumPosition}, read as a big-endian long.
     * Only meaningful for formats that reserved a trailing checksum.
     */
    protected final long getStoredChecksum() {
        try {
            return CodecUtil.readBELong(new ByteArrayDataInput(checksum));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
                assert pos - checksumPosition < STORED_CHECKSUM_LENGTH;
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
}
