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
 * Interface for managing commit-level files (e.g., Lucene segments_N, write.lock).
 * <p>
 * Implementations know which files in the index directory are managed by the commit
 * mechanism and should not be treated as orphans by {@code IndexFileDeleter}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CommitFileManager {
    /**
     * Deletes the commit associated with the given CatalogSnapshot.
     *
     * @param snapshot the snapshot whose backing commit should be deleted
     */
    void deleteCommit(CatalogSnapshot snapshot) throws IOException;

    /**
     * Returns true if the given file is managed by the commit mechanism
     * (e.g., segments_N, write.lock) and should not be treated as an orphan.
     * <p>
     * Implementations MUST return true for all files they manage. Returning false
     * for a commit-managed file will cause {@code IndexFileDeleter} to treat it
     * as an orphan and delete it on startup, leading to data loss.
     *
     * @param fileName the file name to check
     */
    boolean isCommitManagedFile(String fileName);
}
