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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Directory implementation that distributes segment files across multiple
 * subdirectories
 * based on filename hashing. This helps improve I/O distribution by spreading
 * file access
 * across multiple storage paths while maintaining full Lucene Directory
 * compatibility.
 * 
 * Critical files like segments_N are kept in the base directory to maintain
 * compatibility
 * with existing Lucene expectations.
 *
 * @opensearch.internal
 */
public class DistributedSegmentDirectory extends FilterDirectory {

    private static final Logger logger = LogManager.getLogger(DistributedSegmentDirectory.class);

    private final FilenameHasher hasher;
    private final DirectoryManager directoryManager;

    /**
     * Creates a new DistributedSegmentDirectory with default filename hasher.
     *
     * @param delegate the base Directory instance
     * @param basePath the base filesystem path for creating subdirectories
     * @throws IOException if subdirectory creation fails
     */
    public DistributedSegmentDirectory(Directory delegate, Path basePath) throws IOException {
        this(delegate, basePath, new DefaultFilenameHasher());
    }

    /**
     * Creates a new DistributedSegmentDirectory with custom filename hasher.
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

        logger.info("Created DistributedSegmentDirectory with {} subdirectories at path: {}",
                directoryManager.getNumDirectories(), basePath);
    }

    /**
     * Resolves the appropriate directory for a given filename using the hasher.
     *
     * @param filename the filename to resolve
     * @return the Directory instance that should handle this file
     */
    protected Directory resolveDirectory(String filename) {
        int directoryIndex = hasher.getDirectoryIndex(filename);
        return directoryManager.getDirectory(directoryIndex);
    }

    /**
     * Gets the directory index for a filename (useful for logging and debugging).
     *
     * @param filename the filename to check
     * @return the directory index (0-4)
     */
    protected int getDirectoryIndex(String filename) {
        return hasher.getDirectoryIndex(filename);
    }

    /**
     * Gets the DirectoryManager instance (useful for testing).
     *
     * @return the DirectoryManager
     */
    protected DirectoryManager getDirectoryManager() {
        return directoryManager;
    }

    /**
     * Gets the FilenameHasher instance (useful for testing).
     *
     * @return the FilenameHasher
     */
    protected FilenameHasher getHasher() {
        return hasher;
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;

        // Close the directory manager (which closes subdirectories)
        try {
            directoryManager.close();
        } catch (IOException e) {
            exception = e;
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

        logger.debug("Closed DistributedSegmentDirectory");
    }

    // Placeholder methods - will be implemented in subsequent tasks

    @Override
    public String[] listAll() throws IOException {
        Set<String> allFiles = new HashSet<>();
        IOException lastException = null;

        // Collect files from all subdirectories
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

                logger.info("Listed {} files from directory {} (filtered: {})",
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

        // If we couldn't list any directory and have an exception, throw it
        if (allFiles.isEmpty() && lastException != null) {
            throw lastException;
        }

        String[] result = allFiles.toArray(new String[0]);
        logger.info("Listed total {} unique files across all directories", result.length);
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
        int dirIndex = getDirectoryIndex(name);
        Directory targetDir = directoryManager.getDirectory(dirIndex);

        logger.info("Opening input for file {} in directory {}", name, dirIndex);

        try {
            return targetDir.openInput(name, context);
        } catch (IOException e) {
            throw new DistributedDirectoryException(
                    "Failed to open input for file: " + name,
                    dirIndex,
                    "openInput",
                    e);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        int dirIndex = getDirectoryIndex(name);
        Directory targetDir = directoryManager.getDirectory(dirIndex);

        logger.info("Creating output for file {} in directory {}", name, dirIndex);

        try {
            return targetDir.createOutput(name, context);
        } catch (IOException e) {
            throw new DistributedDirectoryException(
                    "Failed to create output for file: " + name,
                    dirIndex,
                    "createOutput",
                    e);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        int dirIndex = getDirectoryIndex(name);
        Directory targetDir = directoryManager.getDirectory(dirIndex);

        logger.info("Deleting file {} from directory {}", name, dirIndex);

        try {
            targetDir.deleteFile(name);
        } catch (IOException e) {
            throw new DistributedDirectoryException(
                    "Failed to delete file: " + name,
                    dirIndex,
                    "deleteFile",
                    e);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        int dirIndex = getDirectoryIndex(name);
        Directory targetDir = directoryManager.getDirectory(dirIndex);

        try {
            return targetDir.fileLength(name);
        } catch (IOException e) {
            throw new DistributedDirectoryException(
                    "Failed to get file length for: " + name,
                    dirIndex,
                    "fileLength",
                    e);
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // Group files by directory and sync each directory separately
        Map<Integer, List<String>> filesByDirectory = names.stream()
                .collect(Collectors.groupingBy(this::getDirectoryIndex));

        List<IOException> exceptions = new ArrayList<>();

        for (Map.Entry<Integer, List<String>> entry : filesByDirectory.entrySet()) {
            int dirIndex = entry.getKey();
            List<String> files = entry.getValue();

            try {
                Directory dir = directoryManager.getDirectory(dirIndex);
                dir.sync(files);
                logger.info("Synced {} files in directory {}", files.size(), dirIndex);
            } catch (IOException e) {
                logger.warn("Failed to sync {} files in directory {}: {}", files.size(), dirIndex, e.getMessage());
                exceptions.add(new DistributedDirectoryException(
                        "Failed to sync files: " + files,
                        dirIndex,
                        "sync",
                        e));
            }
        }

        // If any sync operations failed, throw the first exception with others as
        // suppressed
        if (!exceptions.isEmpty()) {
            IOException primaryException = exceptions.get(0);
            for (int i = 1; i < exceptions.size(); i++) {
                primaryException.addSuppressed(exceptions.get(i));
            }
            throw primaryException;
        }

        logger.info("Successfully synced {} files across {} directories",
                names.size(), filesByDirectory.size());
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        int sourceIndex = getDirectoryIndex(source);
        int destIndex = getDirectoryIndex(dest);

        if (sourceIndex != destIndex) {
            // Cross-directory rename - not supported atomically
            throw new DistributedDirectoryException(
                    "Cross-directory rename not supported: " + source + " (dir " + sourceIndex +
                            ") -> " + dest + " (dir " + destIndex + ")",
                    sourceIndex,
                    "rename");
        }

        Directory targetDir = directoryManager.getDirectory(sourceIndex);

        try {
            targetDir.rename(source, dest);
            logger.info("Renamed {} to {} in directory {}", source, dest, sourceIndex);
        } catch (IOException e) {
            throw new DistributedDirectoryException(
                    "Failed to rename " + source + " to " + dest,
                    sourceIndex,
                    "rename",
                    e);
        }
    }
}