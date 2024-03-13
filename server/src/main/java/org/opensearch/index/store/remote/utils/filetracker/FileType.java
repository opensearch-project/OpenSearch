/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.filetracker;

public enum FileType {
    BLOCK,
    NON_BLOCK;

    public boolean isBlockFile(FileType fileType) {
        return fileType.equals(FileType.BLOCK);
    }
}
