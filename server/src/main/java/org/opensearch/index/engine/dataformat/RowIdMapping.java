/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Mapping interface for translating old row IDs to new row IDs after a merge or sort operation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface RowIdMapping {

    /** Sentinel generation for single-gen flush sort scenarios. */
    long SINGLE_GEN = -1L;

    /**
     * Returns the new row ID corresponding to the given old row ID and generation.
     *
     * @param oldId the original row ID
     * @param oldGeneration the original writer generation
     * @return the new row ID, or -1 if not found
     */
    long getNewRowId(long oldId, long oldGeneration);

    /**
     * Reverse lookup: returns the old row ID corresponding to the given new row ID.
     *
     * @param newId the new row ID
     * @return the original row ID, or -1 if not found
     */
    long getOldRowId(long newId);

    /**
     * Returns whether reverse lookup (getOldRowId / newToOld) is supported by this mapping.
     *
     * @return true if reverse lookup is supported, false otherwise
     */
    boolean isNewToOldSupported();

    /**
     * Returns the total number of documents in this mapping.
     *
     * @return the total document count
     */
    int size();
}
