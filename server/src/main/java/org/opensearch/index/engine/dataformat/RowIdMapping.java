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

    /**
     * Returns the new row ID corresponding to the given old row ID and generation.
     *
     * @param oldId the original row ID
     * @param oldGeneration the original writer generation
     * @return the new row ID, or -1 if not found
     */
    long getNewRowId(long oldId, long oldGeneration);

    /**
     * Returns the new doc position for the given old doc position.
     * Used during single-generation flush sort where generation is implicit.
     * Default is identity (no reordering).
     *
     * @param oldDocId the original document position
     * @return the new document position
     */
    default int oldToNew(int oldDocId) {
        return oldDocId;
    }

    /**
     * Returns the old doc position for the given new doc position.
     * Required by Lucene's Sorter.DocMap for physical reordering.
     *
     * @param newDocId the new document position
     * @return the original document position, or the input if out of range
     */
    default int newToOld(int newDocId) {
        return newDocId;
    }

    /**
     * Returns the total number of documents in this mapping.
     *
     * @return the total document count
     */
    default int size() {
        return 0;
    }
}
