/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Set;

/**
 * Routes segment files to appropriate directories based on primary term.
 * This class determines which directory should handle a given file by either
 * routing to a primary term-specific directory or keeping certain files
 * (like segments_N) in the base directory for compatibility.
 *
 * @opensearch.internal
 */
public class PrimaryTermRouter {

    private static final Logger logger = LogManager.getLogger(PrimaryTermRouter.class);

    /** Files with these prefixes are excluded from primary term routing */
    private static final Set<String> EXCLUDED_PREFIXES = Set.of(
            "segments_",
            "pending_segments_",
            "write.lock");

    private final IndexShardContext shardContext;
    private volatile long lastLoggedPrimaryTerm = -1L;

    /**
     * Creates a new PrimaryTermRouter with the given IndexShardContext.
     *
     * @param shardContext the context for accessing primary term information
     */
    public PrimaryTermRouter(IndexShardContext shardContext) {
        this.shardContext = shardContext;
        logger.debug("Created PrimaryTermRouter with context: {}",
                shardContext != null ? "available" : "null");
    }

    /**
     * Determines the appropriate directory for a given filename.
     * Files are routed based on primary term unless they are excluded
     * (segments_N, lock files, temporary files).
     *
     * @param filename         the name of the file to route
     * @param directoryManager the manager for accessing directories
     * @return the Directory that should handle this file
     * @throws IOException if directory access fails
     */
    public Directory getDirectoryForFile(String filename, PrimaryTermDirectoryManager directoryManager)
            throws IOException {
        if (filename == null || filename.isEmpty()) {
            throw new IllegalArgumentException("Filename cannot be null or empty");
        }

        if (isExcludedFile(filename)) {
            logger.trace("File {} excluded from primary term routing, using base directory", filename);
            return directoryManager.getBaseDirectory();
        }

        long primaryTerm = getCurrentPrimaryTerm();

        // Log primary term changes for debugging
        if (primaryTerm != lastLoggedPrimaryTerm) {
            logger.info("Primary term changed from {} to {} for file routing",
                    lastLoggedPrimaryTerm, primaryTerm);
            lastLoggedPrimaryTerm = primaryTerm;
        }

        try {
            Directory directory = directoryManager.getDirectoryForPrimaryTerm(primaryTerm);
            logger.trace("Routed file {} to primary term {} directory", filename, primaryTerm);
            return directory;
        } catch (IOException e) {
            logger.warn("Failed to get directory for primary term {}, falling back to base directory for file {}",
                    primaryTerm, filename, e);
            return directoryManager.getBaseDirectory();
        }
    }

    /**
     * Gets the current primary term from the IndexShardContext.
     * Returns DEFAULT_PRIMARY_TERM if the context is unavailable.
     *
     * @return the current primary term
     */
    public long getCurrentPrimaryTerm() {
        if (shardContext != null && shardContext.isAvailable()) {
            return shardContext.getPrimaryTerm();
        }

        logger.debug("IndexShardContext unavailable, using default primary term");
        return IndexShardContext.DEFAULT_PRIMARY_TERM;
    }

    /**
     * Checks if a filename should be excluded from primary term routing.
     * Excluded files include:
     * - Files starting with "segments_", "pending_segments_", "write.lock"
     * - Temporary files ending with ".tmp"
     *
     * @param filename the filename to check
     * @return true if the file should be excluded from routing, false otherwise
     */
    public boolean isExcludedFile(String filename) {
        if (filename == null || filename.isEmpty()) {
            return true; // Treat invalid filenames as excluded
        }

        // Exclude temporary files
        if (filename.endsWith(".tmp")) {
            return true;
        }

        // Exclude files with specific prefixes
        return EXCLUDED_PREFIXES.stream().anyMatch(filename::startsWith);
    }

    /**
     * Gets the directory name for a given primary term.
     * Uses the naming convention "primary_term_X" where X is the primary term.
     *
     * @param primaryTerm the primary term
     * @return the directory name for this primary term
     */
    public String getDirectoryNameForPrimaryTerm(long primaryTerm) {
        return PrimaryTermDirectoryManager.PRIMARY_TERM_DIR_PREFIX + primaryTerm;
    }

    /**
     * Gets the IndexShardContext (for testing purposes).
     *
     * @return the IndexShardContext instance
     */
    protected IndexShardContext getShardContext() {
        return shardContext;
    }

    /**
     * Gets the set of excluded prefixes (for testing purposes).
     *
     * @return the set of excluded file prefixes
     */
    protected Set<String> getExcludedPrefixes() {
        return EXCLUDED_PREFIXES;
    }
}