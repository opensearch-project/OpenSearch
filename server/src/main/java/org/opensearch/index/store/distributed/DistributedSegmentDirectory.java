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
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Directory implementation that distributes segment files across multiple
 * subdirectories based on primary term routing. This helps improve I/O distribution 
 * by spreading file access across multiple storage paths while maintaining full 
 * Lucene Directory compatibility and providing better temporal locality.
 * 
 * Critical files like segments_N are kept in the base directory to maintain
 * compatibility with existing Lucene expectations.
 *
 * @opensearch.internal
 */
public class DistributedSegmentDirectory extends FilterDirectory {

    private static final Logger logger = LogManager.getLogger(DistributedSegmentDirectory.class);

    // Legacy fields for backward compatibility
    private final FilenameHasher hasher;
    private final DirectoryManager directoryManager;
    
    // New primary term routing fields
    private final PrimaryTermRouter router;
    private final PrimaryTermDirectoryManager primaryTermDirectoryManager;
    private final boolean usePrimaryTermRouting;

    /**
     * Creates a new DistributedSegmentDirectory with default filename hasher (legacy).
     *
     * @param delegate the base Directory instance
     * @param basePath the base filesystem path for creating subdirectories
     * @throws IOException if subdirectory creation fails
     */
    public DistributedSegmentDirectory(Directory delegate, Path basePath) throws IOException {
        this(delegate, basePath, new DefaultFilenameHasher());
    }

    /**
     * Creates a new DistributedSegmentDirectory with custom filename hasher (legacy).
     *
     * @param delegate the base Directory instance
     * @param basePath the base filesystem path for creating subdirectories
     * @param hasher   the FilenameHasher implementation to use
     * @throws IOException if subdirectory creation fails
     */
    public DistributedSegmentDirectory(Directory delegate, Path basePath, FilenameHasher hasher) throws IOException {
        super(delegate);
        this.hasher = hasher;
        this.directoryManager = new DirectoryManager(delegate, basePath);
        
        // Primary term routing not available in legacy constructor
        this.router = null;
        this.primaryTermDirectoryManager = null;
        this.usePrimaryTermRouting = false;

        logger.info("Created DistributedSegmentDirectory with {} subdirectories at path: {} (legacy hash-based routing)",
                directoryManager.getNumDirectories(), basePath);
    }

    /**
     * Creates a new DistributedSegmentDirectory with primary term routing.
     *
     * @param delegate the base Directory instance
     * @param basePath the base filesystem path for creating subdirectories
     * @param indexShard the IndexShard instance for primary term access
     * @throws IOException if subdirectory creation fails
     */
    public DistributedSegmentDirectory(Directory delegate, Path basePath, IndexShard indexShard) throws IOException {
        super(delegate);
        
        // Initialize primary term routing components
        IndexShardContext shardContext = new IndexShardContext(indexShard);
        this.router = new PrimaryTermRouter(shardContext);
        this.primaryTermDirectoryManager = new PrimaryTermDirectoryManager(delegate, basePath);
        this.usePrimaryTermRouting = true;
        
        // Legacy fields set to null for primary term routing
        this.hasher = null;
        this.directoryManager = null;

        logger.info("Created DistributedSegmentDirectory with primary term routing at path: {}", basePath);
    }

    /**
     * Resolves the appropriate directory for a given filename.
     * Uses primary term routing if available, otherwise falls back to hash-based routing.
     *
     * @param filename the filename to resolve
     * @return the Directory instance that should handle this file
     */
    protected Directory resolveDirectory(String filename) {
        if (usePrimaryTermRouting) {
            try {
                return router.getDirectoryForFile(filename, primaryTermDirectoryManager);
            } catch (IOException e) {
                logger.warn("Primary term routing failed for file {}, falling back to base directory", filename, e);
                return primaryTermDirectoryManager.getBaseDirectory();
            }
        } else {
            // Legacy hash-based routing
            int directoryIndex = hasher.getDirectoryIndex(filename);
            return directoryManager.getDirectory(directoryIndex);
        }
    }

    /**
     * Gets the directory index for a filename (useful for logging and debugging).
     * For primary term routing, returns the primary term as the "index".
     *
     * @param filename the filename to check
     * @return the directory index (0-4) for hash routing, or primary term for primary term routing
     */
    protected int getDirectoryIndex(String filename) {
        if (usePrimaryTermRouting) {
            if (router.isExcludedFile(filename)) {
                return 0; // Base directory
            }
            return (int) router.getCurrentPrimaryTerm();
        } else {
            return hasher.getDirectoryIndex(filename);
        }
    }

    /**
     * Gets the current primary term (for primary term routing).
     *
     * @return the current primary term, or -1 if not using primary term routing
     */
    public long getCurrentPrimaryTerm() {
        if (usePrimaryTermRouting) {
            return router.getCurrentPrimaryTerm();
        }
        return -1L;
    }

    /**
     * Checks if this directory is using primary term routing.
     *
     * @return true if using primary term routing, false if using hash-based routing
     */
    public boolean isUsingPrimaryTermRouting() {
        return usePrimaryTermRouting;
    }

    /**
     * Gets the DirectoryManager instance (useful for testing, legacy routing only).
     *
     * @return the DirectoryManager, or null if using primary term routing
     */
    protected DirectoryManager getDirectoryManager() {
        return directoryManager;
    }

    /**
     * Gets the FilenameHasher instance (useful for testing, legacy routing only).
     *
     * @return the FilenameHasher, or null if using primary term routing
     */
    protected FilenameHasher getHasher() {
        return hasher;
    }

    /**
     * Gets the PrimaryTermRouter instance (useful for testing, primary term routing only).
     *
     * @return the PrimaryTermRouter, or null if using hash-based routing
     */
    protected PrimaryTermRouter getRouter() {
        return router;
    }

    /**
     * Gets the PrimaryTermDirectoryManager instance (useful for testing, primary term routing only).
     *
     * @return the PrimaryTermDirectoryManager, or null if using hash-based routing
     */
    protected PrimaryTermDirectoryManager getPrimaryTermDirectoryManager() {
        return primaryTermDirectoryManager;
    }

    /**
     * Gets detailed routing information for a filename for debugging purposes.
     *
     * @param filename the filename to analyze
     * @return a string describing the routing decision
     */
    public String getRoutingInfo(String filename) {
        if (usePrimaryTermRouting) {
            if (router.isExcludedFile(filename)) {
                return String.format("File '%s' excluded from primary term routing, using base directory", filename);
            } else {
                long primaryTerm = router.getCurrentPrimaryTerm();
                return String.format("File '%s' routed to primary term %d directory", filename, primaryTerm);
            }
        } else {
            int index = hasher.getDirectoryIndex(filename);
            return String.format("File '%s' routed to hash-based directory index %d", filename, index);
        }
    }

    /**
     * Gets statistics about the current directory usage.
     *
     * @return directory usage statistics
     */
    public String getDirectoryStats() {
        if (usePrimaryTermRouting) {
            PrimaryTermDirectoryManager.DirectoryStats stats = primaryTermDirectoryManager.getDirectoryStats();
            return String.format("Primary term routing: %s", stats.toString());
        } else {
            return String.format("Hash-based routing: %d directories", directoryManager.getNumDirectories());
        }
    }

    /**
     * Validates all managed directories for accessibility and health.
     *
     * @throws IOException if validation fails
     */
    public void validateDirectories() throws IOException {
        if (usePrimaryTermRouting) {
            primaryTermDirectoryManager.validateDirectories();
        } else {
            // Legacy validation would go here if DirectoryManager had such a method
            logger.debug("Directory validation not implemented for hash-based routing");
        }
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;

        // Close the appropriate directory manager
        if (usePrimaryTermRouting) {
            try {
                if (primaryTermDirectoryManager != null) {
                    primaryTermDirectoryManager.close();
                }
            } catch (IOException e) {
                exception = e;
            }
        } else {
            try {
                if (directoryManager != null) {
                    directoryManager.close();
                }
            } catch (IOException e) {
                exception = e;
            }
        }

        // Close the base directory
        try {
            super.close();
        } catch (IOException e) {
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
        }

        if (exception != null) {
            throw exception;
        }

        logger.debug("Closed DistributedSegmentDirectory (primary term routing: {})", usePrimaryTermRouting);
    }

    // Placeholder methods - will be implemented in subsequent tasks

    @Override
    public String[] listAll() throws IOException {
        Set<String> allFiles = new HashSet<>();
        IOException lastException = null;

        if (usePrimaryTermRouting) {
            // Primary term routing: collect files from base directory and all primary term directories
            try {
                // Add files from base directory
                Directory baseDir = primaryTermDirectoryManager.getBaseDirectory();
                String[] baseFiles = baseDir.listAll();
                
                for (String file : baseFiles) {
                    // Filter out primary term subdirectories from base directory listing
                    if (!file.startsWith(PrimaryTermDirectoryManager.PRIMARY_TERM_DIR_PREFIX)) {
                        allFiles.add(file);
                    }
                }
                
                logger.debug("Listed {} files from base directory (filtered)", baseFiles.length);
            } catch (IOException e) {
                logger.warn("Failed to list files from base directory: {}", e.getMessage());
                lastException = e;
            }

            // Add files from all primary term directories
            for (Long primaryTerm : primaryTermDirectoryManager.getAllPrimaryTerms()) {
                try {
                    Directory dir = primaryTermDirectoryManager.getDirectoryForPrimaryTerm(primaryTerm);
                    String[] files = dir.listAll();
                    Collections.addAll(allFiles, files);
                    
                    logger.debug("Listed {} files from primary term {} directory", files.length, primaryTerm);
                } catch (IOException e) {
                    logger.warn("Failed to list files from primary term {} directory: {}", primaryTerm, e.getMessage());
                    lastException = e;
                    // Continue with other directories
                }
            }
        } else {
            // Legacy hash-based routing
            for (int i = 0; i < directoryManager.getNumDirectories(); i++) {
                try {
                    Directory dir = directoryManager.getDirectory(i);
                    String[] files = dir.listAll();

                    // Filter out subdirectory names from base directory (index 0)
                    if (i == 0) {
                        for (String file : files) {
                            // Only add files that are not our created subdirectories
                            if (!file.startsWith("varun_segments_") || !isSubdirectoryName(file)) {
                                allFiles.add(file);
                            }
                        }
                    } else {
                        // For other directories, add all files
                        Collections.addAll(allFiles, files);
                    }

                    logger.debug("Listed {} files from directory {} (filtered: {})",
                            files.length, i, i == 0 ? "yes" : "no");
                } catch (IOException e) {
                    logger.warn("Failed to list files from directory {}: {}", i, e.getMessage());
                    lastException = new DistributedDirectoryException(
                            "Failed to list files from directory",
                            i,
                            "listAll",
                            e);
                    // Continue trying other directories
                }
            }
        }

        // If we couldn't list any directory and have an exception, throw it
        if (allFiles.isEmpty() && lastException != null) {
            throw lastException;
        }

        String[] result = allFiles.toArray(new String[0]);
        logger.info("Listed total {} unique files across all directories (routing: {})", 
                   result.length, usePrimaryTermRouting ? "primary-term" : "hash-based");
        return result;
    }

    /**
     * Checks if a filename matches our subdirectory naming pattern.
     * Our subdirectories are named "segments_1", "segments_2", etc.
     *
     * @param filename the filename to check
     * @return true if this is one of our subdirectory names
     */
    private boolean isSubdirectoryName(String filename) {
        if (!filename.startsWith("varun_segments_")) {
            return false;
        }

        String suffix = filename.substring("varun_segments_".length());
        try {
            int dirIndex = Integer.parseInt(suffix);
            // Check if this matches our subdirectory naming pattern (1-4)
            return dirIndex >= 1 && dirIndex < directoryManager.getNumDirectories();
        } catch (NumberFormatException e) {
            // If it's not a number, it's a real segments file, not our subdirectory
            return false;
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        Directory targetDir = resolveDirectory(name);
        
        if (usePrimaryTermRouting) {
            long primaryTerm = router.isExcludedFile(name) ? -1 : router.getCurrentPrimaryTerm();
            logger.debug("Opening input for file {} (primary term: {})", name, primaryTerm);
        } else {
            int dirIndex = getDirectoryIndex(name);
            logger.debug("Opening input for file {} in directory {}", name, dirIndex);
        }

        try {
            return targetDir.openInput(name, context);
        } catch (IOException e) {
            if (usePrimaryTermRouting) {
                throw new PrimaryTermRoutingException(
                        "Failed to open input for file: " + name,
                        PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR,
                        router.getCurrentPrimaryTerm(),
                        name,
                        e);
            } else {
                throw new DistributedDirectoryException(
                        "Failed to open input for file: " + name,
                        getDirectoryIndex(name),
                        "openInput",
                        e);
            }
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        Directory targetDir = resolveDirectory(name);
        
        if (usePrimaryTermRouting) {
            long primaryTerm = router.isExcludedFile(name) ? -1 : router.getCurrentPrimaryTerm();
            logger.debug("Creating output for file {} (primary term: {})", name, primaryTerm);
        } else {
            int dirIndex = getDirectoryIndex(name);
            logger.debug("Creating output for file {} in directory {}", name, dirIndex);
        }

        try {
            return targetDir.createOutput(name, context);
        } catch (IOException e) {
            if (usePrimaryTermRouting) {
                throw new PrimaryTermRoutingException(
                        "Failed to create output for file: " + name,
                        PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR,
                        router.getCurrentPrimaryTerm(),
                        name,
                        e);
            } else {
                throw new DistributedDirectoryException(
                        "Failed to create output for file: " + name,
                        getDirectoryIndex(name),
                        "createOutput",
                        e);
            }
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Directory targetDir = resolveDirectory(name);
        
        if (usePrimaryTermRouting) {
            long primaryTerm = router.isExcludedFile(name) ? -1 : router.getCurrentPrimaryTerm();
            logger.debug("Deleting file {} (primary term: {})", name, primaryTerm);
        } else {
            int dirIndex = getDirectoryIndex(name);
            logger.debug("Deleting file {} from directory {}", name, dirIndex);
        }

        try {
            targetDir.deleteFile(name);
        } catch (IOException e) {
            if (usePrimaryTermRouting) {
                throw new PrimaryTermRoutingException(
                        "Failed to delete file: " + name,
                        PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR,
                        router.getCurrentPrimaryTerm(),
                        name,
                        e);
            } else {
                throw new DistributedDirectoryException(
                        "Failed to delete file: " + name,
                        getDirectoryIndex(name),
                        "deleteFile",
                        e);
            }
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        Directory targetDir = resolveDirectory(name);

        try {
            return targetDir.fileLength(name);
        } catch (IOException e) {
            if (usePrimaryTermRouting) {
                throw new PrimaryTermRoutingException(
                        "Failed to get file length for: " + name,
                        PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR,
                        router.getCurrentPrimaryTerm(),
                        name,
                        e);
            } else {
                throw new DistributedDirectoryException(
                        "Failed to get file length for: " + name,
                        getDirectoryIndex(name),
                        "fileLength",
                        e);
            }
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        List<IOException> exceptions = new ArrayList<>();

        if (usePrimaryTermRouting) {
            // Group files by directory (base directory or primary term directories)
            Map<Directory, List<String>> filesByDirectory = new HashMap<>();
            
            for (String name : names) {
                Directory targetDir = resolveDirectory(name);
                filesByDirectory.computeIfAbsent(targetDir, k -> new ArrayList<>()).add(name);
            }

            for (Map.Entry<Directory, List<String>> entry : filesByDirectory.entrySet()) {
                Directory dir = entry.getKey();
                List<String> files = entry.getValue();

                try {
                    dir.sync(files);
                    logger.debug("Synced {} files in primary term directory", files.size());
                } catch (IOException e) {
                    logger.warn("Failed to sync {} files in primary term directory: {}", files.size(), e.getMessage());
                    exceptions.add(new PrimaryTermRoutingException(
                            "Failed to sync files: " + files,
                            PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR,
                            router.getCurrentPrimaryTerm(),
                            e));
                }
            }
        } else {
            // Legacy hash-based routing
            Map<Integer, List<String>> filesByDirectory = names.stream()
                    .collect(Collectors.groupingBy(this::getDirectoryIndex));

            for (Map.Entry<Integer, List<String>> entry : filesByDirectory.entrySet()) {
                int dirIndex = entry.getKey();
                List<String> files = entry.getValue();

                try {
                    Directory dir = directoryManager.getDirectory(dirIndex);
                    dir.sync(files);
                    logger.debug("Synced {} files in directory {}", files.size(), dirIndex);
                } catch (IOException e) {
                    logger.warn("Failed to sync {} files in directory {}: {}", files.size(), dirIndex, e.getMessage());
                    exceptions.add(new DistributedDirectoryException(
                            "Failed to sync files: " + files,
                            dirIndex,
                            "sync",
                            e));
                }
            }
        }

        // If any sync operations failed, throw the first exception with others as suppressed
        if (!exceptions.isEmpty()) {
            IOException primaryException = exceptions.get(0);
            for (int i = 1; i < exceptions.size(); i++) {
                primaryException.addSuppressed(exceptions.get(i));
            }
            throw primaryException;
        }

        logger.info("Successfully synced {} files across directories (routing: {})",
                names.size(), usePrimaryTermRouting ? "primary-term" : "hash-based");
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        Directory sourceDir = resolveDirectory(source);
        Directory destDir = resolveDirectory(dest);

        if (sourceDir != destDir) {
            // Cross-directory rename - not supported atomically
            if (usePrimaryTermRouting) {
                long sourcePrimaryTerm = router.isExcludedFile(source) ? -1 : router.getCurrentPrimaryTerm();
                long destPrimaryTerm = router.isExcludedFile(dest) ? -1 : router.getCurrentPrimaryTerm();
                throw new PrimaryTermRoutingException(
                        "Cross-directory rename not supported: " + source + " (primary term " + sourcePrimaryTerm +
                                ") -> " + dest + " (primary term " + destPrimaryTerm + ")",
                        PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR,
                        sourcePrimaryTerm,
                        source);
            } else {
                int sourceIndex = getDirectoryIndex(source);
                int destIndex = getDirectoryIndex(dest);
                throw new DistributedDirectoryException(
                        "Cross-directory rename not supported: " + source + " (dir " + sourceIndex +
                                ") -> " + dest + " (dir " + destIndex + ")",
                        sourceIndex,
                        "rename");
            }
        }

        try {
            sourceDir.rename(source, dest);
            if (usePrimaryTermRouting) {
                long primaryTerm = router.isExcludedFile(source) ? -1 : router.getCurrentPrimaryTerm();
                logger.debug("Renamed {} to {} (primary term: {})", source, dest, primaryTerm);
            } else {
                int dirIndex = getDirectoryIndex(source);
                logger.debug("Renamed {} to {} in directory {}", source, dest, dirIndex);
            }
        } catch (IOException e) {
            if (usePrimaryTermRouting) {
                throw new PrimaryTermRoutingException(
                        "Failed to rename " + source + " to " + dest,
                        PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR,
                        router.getCurrentPrimaryTerm(),
                        source,
                        e);
            } else {
                throw new DistributedDirectoryException(
                        "Failed to rename " + source + " to " + dest,
                        getDirectoryIndex(source),
                        "rename",
                        e);
            }
        }
    }
}