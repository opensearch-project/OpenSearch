/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;

/**
 * Functional interface for deleting commit points (e.g., Lucene segments_N files)
 * associated with a {@link CatalogSnapshot}.
 *
 * @opensearch.experimental
 */
@FunctionalInterface
@ExperimentalApi
public interface CommitDeleter {
    /**
     * Deletes the commit associated with the given CatalogSnapshot.
     *
     * @param snapshot the snapshot whose backing commit should be deleted
     */
    void deleteCommit(CatalogSnapshot snapshot) throws IOException;
}
