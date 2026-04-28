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
 * Mapping interface for translating old row IDs to new row IDs after a merge operation.
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
     * @return the new row ID
     */
    long getNewRowId(long oldId, long oldGeneration);
}
