/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

/**
 * Interface for mapping filenames to directory indices in a distributed segment directory.
 * Implementations should provide consistent hashing to ensure the same filename always
 * maps to the same directory index across multiple invocations.
 *
 * @opensearch.internal
 */
public interface FilenameHasher {

    /**
     * Maps a filename to a directory index (0-4 for 5 directories).
     * This method must be deterministic - the same filename should always
     * return the same directory index.
     *
     * @param filename the segment filename to hash
     * @return directory index between 0 and 4 (inclusive)
     * @throws IllegalArgumentException if filename is null or empty
     */
    int getDirectoryIndex(String filename);

    /**
     * Checks if a filename should be excluded from distribution and kept
     * in the base directory (index 0). Typically used for critical files
     * like segments_N that must remain in their expected location.
     *
     * @param filename the filename to check
     * @return true if file should remain in base directory, false if it can be distributed
     */
    boolean isExcludedFile(String filename);
}