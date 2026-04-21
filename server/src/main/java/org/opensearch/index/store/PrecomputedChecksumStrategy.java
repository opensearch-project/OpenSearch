/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

/**
 * Checksum strategy that uses pre-computed checksums when available,
 * falling back to full-file CRC32 scan.
 *
 * <p>The write path registers checksums via {@link #registerChecksum(String, long, long)}.
 * The upload path retrieves them via {@link #computeChecksum(Directory, String)} — O(1)
 * if pre-computed, O(n) fallback otherwise.
 *
 * <p>Each cache entry stores the checksum alongside the writer generation that produced it.
 * This ensures that if a filename is reused (e.g., after merge), the cache always serves
 * the checksum from the latest write, not a stale value from an earlier generation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PrecomputedChecksumStrategy implements FormatChecksumStrategy {

    /** Cache entry: checksum + the generation that produced it. */
    private record CacheEntry(long checksum, long generation) {
    }

    private final Map<String, CacheEntry> checksumCache = new ConcurrentHashMap<>();

    @Override
    public long computeChecksum(Directory dir, String fileName) throws IOException {
        CacheEntry entry = checksumCache.get(fileName);
        if (entry != null) {
            return entry.checksum();
        }
        // Fallback: full-file CRC32 scan
        return computeFullFileCrc32(dir, fileName);
    }

    @Override
    public void registerChecksum(String fileName, long checksum, long writerGeneration) {
        if (fileName != null && checksum != 0) {
            checksumCache.compute(fileName, (key, existing) -> {
                // Only overwrite if the new generation is >= the existing one.
                // This prevents a race where an older generation's late registration
                // overwrites a newer generation's checksum.
                if (existing == null || writerGeneration >= existing.generation()) {
                    return new CacheEntry(checksum, writerGeneration);
                }
                return existing;
            });
        }
    }

    @Override
    public void clearChecksums() {
        checksumCache.clear();
    }

    /**
     * Removes a single checksum entry after it has been consumed (e.g., after successful upload).
     * Prevents unbounded cache growth over the shard's lifetime.
     *
     * @param fileName the file whose checksum should be evicted
     */
    public void evictChecksum(String fileName) {
        if (fileName != null) {
            checksumCache.remove(fileName);
        }
    }

    private static long computeFullFileCrc32(Directory dir, String fileName) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[64 * 1024];
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
