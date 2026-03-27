
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.utils;

import org.apache.lucene.store.Directory;

import java.nio.file.Path;

/**
 * Utility methods for directory path resolution in tiered storage.
 */
public class DirectoryUtils {

    /** Private constructor. */
    private DirectoryUtils() {}

    /** Prefix for switchable file paths. */
    public static final String SWITCHABLE_PREFIX = "_switchable";

    /**
     * Resolves the file path in the given directory.
     * @param localDirectory the local directory
     * @param fileName the file name
     */
    public static Path getFilePath(Directory localDirectory, String fileName) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Resolves the switchable file path in the given directory.
     * @param localDirectory the local directory
     * @param fileName the file name
     */
    public static Path getFilePathSwitchable(Directory localDirectory, String fileName) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
