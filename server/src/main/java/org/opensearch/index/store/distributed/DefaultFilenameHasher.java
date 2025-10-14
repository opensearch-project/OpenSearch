/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import java.util.Set;

/**
 * Default implementation of FilenameHasher that uses consistent hashing
 * to distribute segment files across multiple directories while keeping
 * critical files like segments_N in the base directory.
 *
 * @opensearch.internal
 */
public class DefaultFilenameHasher implements FilenameHasher {

    private static final int NUM_DIRECTORIES = 5;
    private static final Set<String> EXCLUDED_PREFIXES = Set.of("segments_", "pending_segments_", "write.lock");

    /**
     * Maps a filename to a directory index using consistent hashing.
     * Files with excluded prefixes (like segments_N) are always mapped to index 0.
     *
     * @param filename the segment filename to hash
     * @return directory index between 0 and 4 (inclusive)
     * @throws IllegalArgumentException if filename is null or empty
     */
    @Override
    public int getDirectoryIndex(String filename) {
        if (filename == null || filename.isEmpty()) {
            throw new IllegalArgumentException("Filename cannot be null or empty");
        }

        if (isExcludedFile(filename)) {
            return 0; // Base directory for excluded files
        }

        // Use consistent hashing with absolute value to ensure positive result
        return Math.abs(filename.hashCode()) % NUM_DIRECTORIES;
    }

    /**
     * Checks if a filename should be excluded from distribution.
     * Currently excludes files starting with "segments_" prefix.
     *
     * @param filename the filename to check
     * @return true if file should remain in base directory, false if it can be distributed
     */
    @Override
    public boolean isExcludedFile(String filename) {
        if (filename == null || filename.isEmpty()) {
            return true; // Treat invalid filenames as excluded
        }

        if (filename.endsWith(".tmp")) {
            return true;
        }

        return EXCLUDED_PREFIXES.stream().anyMatch(filename::startsWith);
    }
}