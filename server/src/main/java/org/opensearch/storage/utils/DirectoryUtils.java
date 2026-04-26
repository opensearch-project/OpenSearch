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

import java.nio.file.Path;

/**
 * Utility methods for directory path resolution in tiered storage.
 */
public class DirectoryUtils {

    public static final String SWITCHABLE_PREFIX = "_switchable";

    /**
     * Walks the {@link FilterDirectory} chain to find the underlying {@link FSDirectory}.
     * Returns immediately if the given directory is already an FSDirectory.
     *
     * @param dir the directory to unwrap
     * @return the underlying FSDirectory
     * @throws IllegalStateException if no FSDirectory is found in the chain
     */
    public static FSDirectory getFSDirectory(Directory dir) {
        Directory current = dir;
        while (current instanceof FilterDirectory) {
            current = ((FilterDirectory) current).getDelegate();
        }
        if (current instanceof FSDirectory) {
            return (FSDirectory) current;
        }
        throw new IllegalStateException("No FSDirectory found in directory chain: " + dir.getClass().getName());
    }

    public static Path getFilePath(Directory localDirectory, String fileName) {
        return getFSDirectory(localDirectory).getDirectory().resolve(fileName);
    }

    public static Path getFilePathSwitchable(Directory localDirectory, String fileName) {
        return getFSDirectory(localDirectory).getDirectory().resolve(fileName + SWITCHABLE_PREFIX);
    }
}
