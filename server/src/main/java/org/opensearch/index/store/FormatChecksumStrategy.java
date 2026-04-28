/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Strategy for computing file checksums, with support for pre-computed values.
 *
 * <p>Each data format provides its own strategy:
 * <ul>
 *   <li>Lucene: reads the codec footer (O(1))</li>
 *   <li>Parquet: uses pre-computed CRC32 from the Rust writer (O(1)),
 *       falls back to full-file scan if not available</li>
 *   <li>Default: full-file CRC32 scan (O(n))</li>
 * </ul>
 *
 * <p>Pre-computed checksums are registered via {@link #registerChecksum(String, long, long)}
 * during the flush/write path, then consumed by the upload path via
 * {@link #computeChecksum(Directory, String)}.
 *
 * <p>The {@code writerGeneration} parameter in {@link #registerChecksum} ensures that
 * if a filename is reused across generations (e.g., after merge or rewrite), the cache
 * entry is always from the latest write. The generation acts as a version stamp — a
 * stale entry from an older generation is overwritten, never served.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface FormatChecksumStrategy {

    /**
     * Computes or retrieves the checksum for the given file.
     *
     * @param dir the directory containing the file
     * @param fileName the file name (local name, not format-prefixed)
     * @return the checksum value
     * @throws IOException if checksum computation fails
     */
    long computeChecksum(Directory dir, String fileName) throws IOException;

    /**
     * Registers a pre-computed checksum for a file at a specific writer generation.
     * Called during the write/flush path when the checksum is known.
     *
     * <p>The generation parameter ensures cache correctness: if the same filename
     * is written by a later generation, the new checksum replaces the old one.
     * Implementations should store the generation alongside the checksum and
     * reject lookups for stale generations.
     *
     * @param fileName the file name
     * @param checksum the pre-computed checksum
     * @param writerGeneration the writer generation that produced this file
     */
    default void registerChecksum(String fileName, long checksum, long writerGeneration) {}

    /**
     * Clears all cached checksums. Called during cleanup/close.
     */
    default void clearChecksums() {}

}
