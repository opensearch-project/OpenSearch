/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.store.FormatChecksumStrategy;

import java.io.IOException;
import java.util.zip.CRC32;

/**
 * Checksum strategy that computes CRC32 over the entire file contents.
 *
 * <p>This is the default/fallback strategy for non-Lucene formats (Parquet, Arrow, etc.)
 * that do not embed a Lucene codec footer. It reads the entire file — O(n) complexity.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class GenericCRC32ChecksumHandler implements FormatChecksumStrategy {

    private static final int BUFFER_SIZE = 8192;

    @Override
    public long computeChecksum(Directory dir, String fileName) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[BUFFER_SIZE];
        try (IndexInput input = dir.openInput(fileName, IOContext.READONCE)) {
            long remaining = input.length();
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                input.readBytes(buffer, 0, toRead);
                crc32.update(buffer, 0, toRead);
                remaining -= toRead;
            }
        }
        return crc32.getValue();
    }
}
