/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Manages reference counting for index files to prevent premature deletion.
 * Tracks which files are in use by catalog snapshots and ensures files are only
 * deleted when no longer referenced by any snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexFileReferenceManager {
    /**
     * Adds file references for all files in the given catalog snapshot.
     * Increments reference counts to prevent deletion while the snapshot is in use.
     *
     * @param snapshot the catalog snapshot whose files should be referenced
     */
    void addFileReferences(CatalogSnapshot snapshot);

    /**
     * Removes file references for all files in the given catalog snapshot.
     * Decrements reference counts and may trigger deletion of unreferenced files.
     *
     * @param snapshot the catalog snapshot whose file references should be removed
     */
    void removeFileReferences(CatalogSnapshot snapshot);
}
