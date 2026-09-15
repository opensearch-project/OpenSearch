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
 * <p>Each {@code RowIdMapping} instance represents the mapping for a <b>single generation</b>.
 * For merge operations involving multiple generations, callers maintain a
 * {@code Map<Long, RowIdMapping>} keyed by writer generation and pass the appropriate
 * per-generation mapping to each consumer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface RowIdMapping {

    /**
     * Returns the new row ID corresponding to the given old row ID within this generation.
     *
     * @param oldId the original row ID
     * @return the new row ID, or -1 if not found
     */
    long getNewRowId(long oldId);

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
