/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.utils;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.annotation.ExperimentalApi;

import java.nio.file.Path;

/**
 * Utility methods for directory path resolution in tiered storage.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DirectoryUtils {

    /** Suffix for switchable file paths. */
    public static final String SWITCHABLE_PREFIX = "_switchable";

    /**
     * Walks the {@link FilterDirectory} chain to find the underlying {@link FSDirectory}.
     * Returns immediately if the given directory is already an FSDirectory.
     *
     * @param directory the directory to unwrap
     * @return the underlying FSDirectory
     * @throws IllegalArgumentException if no FSDirectory is found in the chain
     */
    public static FSDirectory unwrapFSDirectory(Directory directory) {
        Directory current = directory;
        while (current instanceof FilterDirectory) {
            current = ((FilterDirectory) current).getDelegate();
        }
        if (current instanceof FSDirectory) {
            return (FSDirectory) current;
        }
        throw new IllegalArgumentException("Expected FSDirectory but got: " + directory.getClass().getName());
    }

    /**
     * Resolves the file path for a given file name in the directory.
     * @param localDirectory the directory
     * @param fileName the file name
     * @return the resolved path
     */
    public static Path getFilePath(Directory localDirectory, String fileName) {
        return unwrapFSDirectory(localDirectory).getDirectory().resolve(fileName);
    }

    /**
     * Resolves the switchable file path for a given file name in the directory.
     * @param localDirectory the directory
     * @param fileName the file name
     * @return the resolved switchable path
     */
    public static Path getFilePathSwitchable(Directory localDirectory, String fileName) {
        return unwrapFSDirectory(localDirectory).getDirectory().resolve(fileName + SWITCHABLE_PREFIX);
    }
}
