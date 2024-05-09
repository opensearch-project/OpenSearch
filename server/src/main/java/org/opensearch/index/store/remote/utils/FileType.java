/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Enum to represent whether a file is block or not
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum FileType {
    /**
     * Block file
     */
    BLOCK(".*_block_.*"),
    /**
     * Full file - Non-Block
     */
    FULL(".*");

    private final String pattern;

    FileType(String pattern) {
        this.pattern = pattern;
    }

    /**
     * Returns if the fileType is a block file or not
     */
    public static boolean isBlockFile(FileType fileType) {
        return fileType.equals(FileType.BLOCK);
    }

    /**
     * Returns if the fileName is block file or not
     */
    public static boolean isBlockFile(String fileName) {
        if (fileName.matches(FileType.BLOCK.pattern)) return true;
        return false;
    }
}
