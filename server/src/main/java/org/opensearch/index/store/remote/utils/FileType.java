/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

/**
 * Enum to represent whether a file is block or not
 */
public enum FileType {
    /**
     * Block file
     */
    BLOCK,
    /**
     * Non block file
     */
    NON_BLOCK;

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
        if (fileName.contains("_block_")) return true;
        return false;
    }
}
