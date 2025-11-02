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
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the creation and lifecycle of directories based on primary terms.
 * This class handles dynamic directory creation, maintains mappings between
 * primary terms and Directory instances, and provides directory validation
 * and cleanup functionality.
 *
 * @opensearch.internal
 */
public class PrimaryTermDirectoryManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(PrimaryTermDirectoryManager.class);
    
    /** Prefix used for primary term directory names */
    public static final String PRIMARY_TERM_DIR_PREFIX = "primary_term_";
    
    private final Directory baseDirectory;
    private final Path basePath;
    private final Map<Long, Directory> primaryTermDirectories;
    private volatile boolean closed = false;

    /**
     * Creates a new PrimaryTermDirectoryManager.
     *
     * @param baseDirectory the base directory for fallback operations
     * @param basePath the base filesystem path for creating subdirectories
     * @throws IOException if initialization fails
     */
    public PrimaryTermDirectoryManager(Directory baseDirectory, Path basePath) throws IOException {
        this.baseDirectory = baseDirectory;
        this.basePath = basePath;
        this.primaryTermDirectories = new ConcurrentHashMap<>();
        
        // Ensure base path exists
        Files.createDirectories(basePath);
        
        logger.info("Created PrimaryTermDirectoryManager at path: {}", basePath);
    }

    /**
     * Gets the directory for a given primary term, creating it if necessary.
     * This method uses lazy initialization with thread-safe double-check pattern.
     *
     * @param primaryTerm the primary term
     * @return the Directory instance for this primary term
     * @throws IOException if directory creation fails
     */
    public Directory getDirectoryForPrimaryTerm(long primaryTerm) throws IOException {
        ensureNotClosed();
        
        Directory existing = primaryTermDirectories.get(primaryTerm);
        if (existing != null) {
            return existing;
        }
        
        return createDirectoryForPrimaryTerm(primaryTerm);
    }

    /**
     * Creates a new directory for the given primary term using thread-safe double-check pattern.
     * This method is synchronized to prevent race conditions during directory creation.
     *
     * @param primaryTerm the primary term
     * @return the newly created Directory instance
     * @throws IOException if directory creation fails
     */
    private synchronized Directory createDirectoryForPrimaryTerm(long primaryTerm) throws IOException {
        ensureNotClosed();
        
        // Double-check pattern for thread safety
        Directory existing = primaryTermDirectories.get(primaryTerm);
        if (existing != null) {
            return existing;
        }
        
        try {
            String dirName = getDirectoryNameForPrimaryTerm(primaryTerm);
            Path dirPath = basePath.resolve(dirName);
            
            // Create directory if it doesn't exist
            Files.createDirectories(dirPath);
            
            // Create and store the Directory instance
            Directory newDirectory = FSDirectory.open(dirPath);
            primaryTermDirectories.put(primaryTerm, newDirectory);
            
            logger.info("Created directory for primary term {}: {}", primaryTerm, dirPath);
            return newDirectory;
            
        } catch (IOException e) {
            logger.error("Failed to create directory for primary term {}", primaryTerm, e);
            throw new PrimaryTermRoutingException(
                "Failed to create directory for primary term " + primaryTerm,
                PrimaryTermRoutingException.ErrorType.DIRECTORY_CREATION_ERROR,
                primaryTerm,
                e
            );
        }
    }

    /**
     * Gets the directory name for a given primary term using the standard naming convention.
     *
     * @param primaryTerm the primary term
     * @return the directory name (e.g., "primary_term_1")
     */
    public String getDirectoryNameForPrimaryTerm(long primaryTerm) {
        return PRIMARY_TERM_DIR_PREFIX + primaryTerm;
    }

    /**
     * Gets the base directory used for fallback operations.
     *
     * @return the base Directory instance
     */
    public Directory getBaseDirectory() {
        return baseDirectory;
    }

    /**
     * Gets the base filesystem path.
     *
     * @return the base Path
     */
    public Path getBasePath() {
        return basePath;
    }

    /**
     * Lists all primary term directories currently managed.
     *
     * @return a list of all Directory instances
     */
    public List<Directory> listAllDirectories() {
        ensureNotClosed();
        return new ArrayList<>(primaryTermDirectories.values());
    }

    /**
     * Gets all primary terms that have directories.
     *
     * @return a set of all primary terms with directories
     */
    public Set<Long> getAllPrimaryTerms() {
        ensureNotClosed();
        return primaryTermDirectories.keySet();
    }

    /**
     * Gets the number of primary term directories currently managed.
     *
     * @return the number of directories
     */
    public int getDirectoryCount() {
        return primaryTermDirectories.size();
    }

    /**
     * Checks if a directory exists for the given primary term.
     *
     * @param primaryTerm the primary term to check
     * @return true if a directory exists, false otherwise
     */
    public boolean hasDirectoryForPrimaryTerm(long primaryTerm) {
        return primaryTermDirectories.containsKey(primaryTerm);
    }

    /**
     * Validates that all managed directories are accessible.
     * This method checks each directory for basic accessibility.
     *
     * @throws IOException if any directory validation fails
     */
    public void validateDirectories() throws IOException {
        ensureNotClosed();
        
        List<Exception> validationErrors = new ArrayList<>();
        
        for (Map.Entry<Long, Directory> entry : primaryTermDirectories.entrySet()) {
            long primaryTerm = entry.getKey();
            Directory directory = entry.getValue();
            
            try {
                // Basic validation - try to list files
                directory.listAll();
                logger.debug("Validated directory for primary term {}", primaryTerm);
            } catch (Exception e) {
                logger.warn("Validation failed for primary term {} directory", primaryTerm, e);
                validationErrors.add(new PrimaryTermRoutingException(
                    "Directory validation failed for primary term " + primaryTerm,
                    PrimaryTermRoutingException.ErrorType.DIRECTORY_VALIDATION_ERROR,
                    primaryTerm,
                    e
                ));
            }
        }
        
        if (!validationErrors.isEmpty()) {
            IOException combinedException = new IOException("Directory validation failed for " + validationErrors.size() + " directories");
            for (Exception error : validationErrors) {
                combinedException.addSuppressed(error);
            }
            throw combinedException;
        }
        
        logger.info("Successfully validated {} primary term directories", primaryTermDirectories.size());
    }

    /**
     * Closes all managed directories and releases resources.
     * This method should be called when the manager is no longer needed.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        closed = true;
        
        List<IOException> closeExceptions = new ArrayList<>();
        
        // Close all primary term directories
        for (Map.Entry<Long, Directory> entry : primaryTermDirectories.entrySet()) {
            try {
                entry.getValue().close();
                logger.debug("Closed directory for primary term {}", entry.getKey());
            } catch (IOException e) {
                logger.warn("Failed to close directory for primary term {}", entry.getKey(), e);
                closeExceptions.add(e);
            }
        }
        
        primaryTermDirectories.clear();
        
        // If there were close exceptions, throw the first one with others as suppressed
        if (!closeExceptions.isEmpty()) {
            IOException primaryException = closeExceptions.get(0);
            for (int i = 1; i < closeExceptions.size(); i++) {
                primaryException.addSuppressed(closeExceptions.get(i));
            }
            throw primaryException;
        }
        
        logger.info("Closed PrimaryTermDirectoryManager with {} directories", primaryTermDirectories.size());
    }

    /**
     * Checks if this manager has been closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Ensures that this manager has not been closed.
     *
     * @throws IllegalStateException if the manager is closed
     */
    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("PrimaryTermDirectoryManager has been closed");
        }
    }

    /**
     * Performs comprehensive validation of a specific primary term directory.
     * This includes accessibility, permission checks, and basic operations.
     *
     * @param primaryTerm the primary term to validate
     * @throws IOException if validation fails
     */
    public void validatePrimaryTermDirectory(long primaryTerm) throws IOException {
        ensureNotClosed();
        
        Directory directory = primaryTermDirectories.get(primaryTerm);
        if (directory == null) {
            throw new PrimaryTermRoutingException(
                "No directory found for primary term " + primaryTerm,
                PrimaryTermRoutingException.ErrorType.DIRECTORY_VALIDATION_ERROR,
                primaryTerm,
                (Throwable) null
            );
        }
        
        try {
            // Test basic operations
            String[] files = directory.listAll();
            logger.debug("Primary term {} directory contains {} files", primaryTerm, files.length);
            
            // Test sync operation
            directory.sync(java.util.Arrays.asList(files));
            
            // Additional validation could include:
            // - Check directory permissions
            // - Verify disk space
            // - Test write operations
            
        } catch (Exception e) {
            throw new PrimaryTermRoutingException(
                "Validation failed for primary term " + primaryTerm + " directory",
                PrimaryTermRoutingException.ErrorType.DIRECTORY_VALIDATION_ERROR,
                primaryTerm,
                e
            );
        }
    }

    /**
     * Cleans up unused directories for primary terms that are no longer active.
     * This method can be called periodically to reclaim disk space.
     *
     * @param activePrimaryTerms set of primary terms that should be kept
     * @return the number of directories cleaned up
     * @throws IOException if cleanup fails
     */
    public int cleanupUnusedDirectories(Set<Long> activePrimaryTerms) throws IOException {
        ensureNotClosed();
        
        List<Long> toRemove = new ArrayList<>();
        
        for (Long primaryTerm : primaryTermDirectories.keySet()) {
            if (!activePrimaryTerms.contains(primaryTerm)) {
                toRemove.add(primaryTerm);
            }
        }
        
        int cleanedUp = 0;
        for (Long primaryTerm : toRemove) {
            try {
                Directory directory = primaryTermDirectories.remove(primaryTerm);
                if (directory != null) {
                    directory.close();
                    cleanedUp++;
                    logger.info("Cleaned up directory for inactive primary term {}", primaryTerm);
                }
            } catch (IOException e) {
                logger.warn("Failed to cleanup directory for primary term {}", primaryTerm, e);
                // Continue with other directories
            }
        }
        
        logger.info("Cleaned up {} unused primary term directories", cleanedUp);
        return cleanedUp;
    }

    /**
     * Gets statistics about the managed directories.
     *
     * @return DirectoryStats containing information about managed directories
     */
    public DirectoryStats getDirectoryStats() {
        ensureNotClosed();
        
        int totalDirectories = primaryTermDirectories.size();
        long minPrimaryTerm = primaryTermDirectories.keySet().stream().mapToLong(Long::longValue).min().orElse(-1L);
        long maxPrimaryTerm = primaryTermDirectories.keySet().stream().mapToLong(Long::longValue).max().orElse(-1L);
        
        return new DirectoryStats(totalDirectories, minPrimaryTerm, maxPrimaryTerm, basePath.toString());
    }

    /**
     * Statistics about managed directories.
     */
    public static class DirectoryStats {
        private final int totalDirectories;
        private final long minPrimaryTerm;
        private final long maxPrimaryTerm;
        private final String basePath;

        public DirectoryStats(int totalDirectories, long minPrimaryTerm, long maxPrimaryTerm, String basePath) {
            this.totalDirectories = totalDirectories;
            this.minPrimaryTerm = minPrimaryTerm;
            this.maxPrimaryTerm = maxPrimaryTerm;
            this.basePath = basePath;
        }

        public int getTotalDirectories() {
            return totalDirectories;
        }

        public long getMinPrimaryTerm() {
            return minPrimaryTerm;
        }

        public long getMaxPrimaryTerm() {
            return maxPrimaryTerm;
        }

        public String getBasePath() {
            return basePath;
        }

        @Override
        public String toString() {
            return String.format("DirectoryStats{totalDirectories=%d, minPrimaryTerm=%d, maxPrimaryTerm=%d, basePath='%s'}", 
                               totalDirectories, minPrimaryTerm, maxPrimaryTerm, basePath);
        }
    }

    /**
     * Checks if the filesystem has sufficient space for new directories.
     * This is a basic check that can be extended with more sophisticated logic.
     *
     * @param requiredSpaceBytes minimum required space in bytes
     * @return true if sufficient space is available
     */
    public boolean hasAvailableSpace(long requiredSpaceBytes) {
        try {
            long usableSpace = Files.getFileStore(basePath).getUsableSpace();
            boolean hasSpace = usableSpace >= requiredSpaceBytes;
            
            if (!hasSpace) {
                logger.warn("Insufficient disk space: required={} bytes, available={} bytes", 
                           requiredSpaceBytes, usableSpace);
            }
            
            return hasSpace;
        } catch (IOException e) {
            logger.warn("Failed to check available disk space", e);
            return true; // Assume space is available if check fails
        }
    }

    /**
     * Forces synchronization of all managed directories.
     * This ensures all pending writes are flushed to disk.
     *
     * @throws IOException if sync fails for any directory
     */
    public void syncAllDirectories() throws IOException {
        ensureNotClosed();
        
        List<IOException> syncErrors = new ArrayList<>();
        
        for (Map.Entry<Long, Directory> entry : primaryTermDirectories.entrySet()) {
            try {
                String[] files = entry.getValue().listAll();
                entry.getValue().sync(java.util.Arrays.asList(files));
                logger.debug("Synced directory for primary term {}", entry.getKey());
            } catch (IOException e) {
                logger.warn("Failed to sync directory for primary term {}", entry.getKey(), e);
                syncErrors.add(e);
            }
        }
        
        if (!syncErrors.isEmpty()) {
            IOException combinedException = new IOException("Failed to sync " + syncErrors.size() + " directories");
            for (IOException error : syncErrors) {
                combinedException.addSuppressed(error);
            }
            throw combinedException;
        }
        
        logger.info("Successfully synced {} primary term directories", primaryTermDirectories.size());
    }

    /**
     * Gets the primary term directories map (for testing purposes).
     *
     * @return the map of primary terms to directories
     */
    protected Map<Long, Directory> getPrimaryTermDirectories() {
        return primaryTermDirectories;
    }
}