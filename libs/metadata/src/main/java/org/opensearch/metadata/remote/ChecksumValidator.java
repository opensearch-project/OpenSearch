/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.remote;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Arrays;

/**
 * Validates Lucene checksums for remote metadata blobs and extracts content.
 * Uses the same format as ChecksumBlobStoreFormat in the server module:
 * [Header: codec name + version][Content][Footer: checksum].
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ChecksumValidator {

    /**
     * Creates a new ChecksumValidator instance.
     */
    public ChecksumValidator() {}

    /**
     * Validates the checksum and extracts the content as a {@link BytesReference} slice.
     * This is a zero-copy operation — the returned reference shares the underlying bytes.
     *
     * @param bytes      the raw blob data with Lucene header/footer
     * @param codecName  the expected codec name
     * @param minVersion the minimum supported version
     * @param maxVersion the maximum supported version
     * @return the content bytes without header/footer as a BytesReference slice
     * @throws CorruptMetadataException if checksum validation fails
     * @throws IOException              if an I/O error occurs
     */
    public BytesReference validateAndSlice(BytesReference bytes, String codecName, int minVersion, int maxVersion) throws IOException {
        return validateAndSlice(
            bytes,
            codecName,
            minVersion,
            maxVersion,
            "ChecksumValidator.validateAndSlice(codec=\"" + codecName + "\")"
        );
    }

    /**
     * Validates the checksum and extracts the content as a {@link BytesReference} slice,
     * using the given resource description in error messages.
     *
     * @param bytes        the raw blob data with Lucene header/footer
     * @param codecName    the expected codec name
     * @param minVersion   the minimum supported version
     * @param maxVersion   the maximum supported version
     * @param resourceDesc description for error messages (e.g., blob name)
     * @return the content bytes without header/footer as a BytesReference slice
     * @throws CorruptMetadataException if checksum validation fails
     * @throws IOException              if an I/O error occurs
     */
    public BytesReference validateAndSlice(BytesReference bytes, String codecName, int minVersion, int maxVersion, String resourceDesc)
        throws IOException {
        if (bytes == null || bytes.length() == 0) {
            throw new CorruptMetadataException("Empty or null data provided for checksum validation");
        }

        try {
            IndexInput indexInput = new ByteBuffersIndexInput(
                new ByteBuffersDataInput(Arrays.asList(BytesReference.toByteBuffers(bytes))),
                resourceDesc
            );

            CodecUtil.checksumEntireFile(indexInput);
            CodecUtil.checkHeader(indexInput, codecName, minVersion, maxVersion);

            long contentStart = indexInput.getFilePointer();
            long contentSize = indexInput.length() - CodecUtil.footerLength() - contentStart;

            if (contentSize < 0) {
                throw new CorruptMetadataException("Invalid content size: " + contentSize);
            }

            return bytes.slice((int) contentStart, (int) contentSize);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            throw new CorruptMetadataException("Metadata checksum validation failed: " + ex.getMessage(), ex);
        }
    }
}
