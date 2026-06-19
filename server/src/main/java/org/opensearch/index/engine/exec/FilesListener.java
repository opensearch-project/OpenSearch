/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Collection;

/**
 * Listener notified when files are added to or deleted from a data format's storage.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface FilesListener {
    /**
     * Called when new files become visible (ref count went from 0 to 1).
     *
     * @param files the file names that were added
     */
    void onFilesDeleted(Collection<String> files) throws IOException;

    /**
     * Called when files are deleted (ref count reached 0).
     *
     * @param files the file names that were deleted
     */
    void onFilesAdded(Collection<String> files) throws IOException;
}
