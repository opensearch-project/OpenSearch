/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Manages the creation and access to subdirectories for distributed segment storage.
 * Creates up to 5 subdirectories for distributing segment files while keeping
 * the base directory (index 0) for critical files like segments_N.
 *
 * @opensearch.internal
 */
public class DirectoryManager {

    private static final int NUM_DIRECTORIES = 5;
    private final Path baseDirectory;
    private final Directory[] subdirectories;

    /**
     * Creates a new DirectoryManager with the specified base directory.
     *
     * @param baseDirectory the base Directory instance (used as subdirectory 0)
     * @param basePath the base filesystem path for creating subdirectories
     * @throws IOException if subdirectory creation fails
     */
    public DirectoryManager(Directory baseDirectory, Path basePath) throws IOException {
        this.baseDirectory = basePath;
        this.subdirectories = createSubdirectories(baseDirectory, basePath);
    }

    /**
     * Creates the array of subdirectories, with index 0 being the base directory
     * and indices 1-4 being newly created subdirectories.
     *
     * @param base the base Directory instance
     * @param basePath the base filesystem path
     * @return array of Directory instances
     * @throws IOException if subdirectory creation fails
     */
    private Directory[] createSubdirectories(Directory base, Path basePath) throws IOException {
        Directory[] dirs = new Directory[NUM_DIRECTORIES];
        dirs[0] = base; // Base directory for segments_N and excluded files

        try {
            for (int i = 1; i < NUM_DIRECTORIES; i++) {
                Path subPath = basePath.resolve("varun_segments_" + i);
                
                // Create directory if it doesn't exist
                if (!Files.exists(subPath)) {
                    Files.createDirectories(subPath);
                }
                
                // Validate directory is writable
                if (!Files.isWritable(subPath)) {
                    throw new IOException("Subdirectory is not writable: " + subPath);
                }
                
                dirs[i] = new NIOFSDirectory(subPath, FSLockFactory.getDefault());
            }
        } catch (IOException e) {
            // Clean up any successfully created directories
            closeDirectories(dirs);
            throw new DistributedDirectoryException(
                "Failed to create subdirectories", 
                -1, 
                "createSubdirectories", 
                e
            );
        }

        return dirs;
    }

    /**
     * Gets the Directory instance for the specified index.
     *
     * @param index the directory index (0-4)
     * @return the Directory instance
     * @throws IllegalArgumentException if index is out of range
     */
    public Directory getDirectory(int index) {
        if (index < 0 || index >= NUM_DIRECTORIES) {
            throw new IllegalArgumentException("Directory index must be between 0 and " + (NUM_DIRECTORIES - 1) + 
                                             ", got: " + index);
        }
        return subdirectories[index];
    }

    /**
     * Gets the number of managed directories.
     *
     * @return the number of directories (always 5)
     */
    public int getNumDirectories() {
        return NUM_DIRECTORIES;
    }

    /**
     * Gets the base filesystem path.
     *
     * @return the base path
     */
    public Path getBasePath() {
        return baseDirectory;
    }

    /**
     * Closes all managed directories except the base directory (index 0).
     * The base directory should be closed by the caller since it was provided
     * during construction.
     *
     * @throws IOException if any directory fails to close
     */
    public void close() throws IOException {
        closeDirectories(subdirectories);
    }

    /**
     * Helper method to close directories and collect exceptions.
     *
     * @param dirs array of directories to close
     * @throws IOException if any directory fails to close
     */
    private void closeDirectories(Directory[] dirs) throws IOException {
        IOException exception = null;

        // Close subdirectories (skip index 0 as it's the base directory managed externally)
        for (int i = 1; i < dirs.length && dirs[i] != null; i++) {
            try {
                dirs[i].close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = new DistributedDirectoryException(
                        "Failed to close subdirectory", 
                        i, 
                        "close", 
                        e
                    );
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }
}