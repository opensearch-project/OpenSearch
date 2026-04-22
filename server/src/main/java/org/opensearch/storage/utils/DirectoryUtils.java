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

import java.nio.file.Path;

/**
 * Utility methods for directory path resolution in tiered storage.
 */
public class DirectoryUtils {

    public static final String SWITCHABLE_PREFIX = "_switchable";

    public static Path getFilePath(Directory localDirectory, String fileName) {
        return ((FSDirectory) localDirectory).getDirectory().resolve(fileName);
    }

    public static Path getFilePathSwitchable(Directory localDirectory, String fileName) {
        return ((FSDirectory) localDirectory).getDirectory().resolve(fileName + SWITCHABLE_PREFIX);
    }
}
