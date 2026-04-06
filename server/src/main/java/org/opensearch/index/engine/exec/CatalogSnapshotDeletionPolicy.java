/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Policy for deleting old committed CatalogSnapshots.
 * Analogous to Lucene's {@link org.apache.lucene.index.IndexDeletionPolicy}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CatalogSnapshotDeletionPolicy {

    /** A policy that keeps only the latest commit and deletes all older ones. */
    CatalogSnapshotDeletionPolicy KEEP_LATEST_ONLY = new CatalogSnapshotDeletionPolicy() {
        @Override
        public List<CatalogSnapshot> onInit(List<CatalogSnapshot> commits) {
            return Collections.emptyList();
        }

        @Override
        public List<CatalogSnapshot> onCommit(List<CatalogSnapshot> commits) {
            if (commits.size() <= 1) {
                return Collections.emptyList();
            }
            return new ArrayList<>(commits.subList(0, commits.size() - 1));
        }
    };

    /**
     * Called on engine open with the list of existing committed snapshots.
     *
     * @param commits all committed snapshots, ordered oldest first
     * @return list of snapshots that should be deleted
     */
    List<CatalogSnapshot> onInit(List<CatalogSnapshot> commits) throws IOException;

    /**
     * Called after every flush/commit with the full list of committed snapshots.
     *
     * @param commits all committed snapshots, ordered oldest first
     * @return list of snapshots that should be deleted
     */
    List<CatalogSnapshot> onCommit(List<CatalogSnapshot> commits) throws IOException;

    /**
     * Acquire the most recent safe or last committed snapshot.
     * The snapshot won't be deleted until the returned {@link GatedCloseable} is closed.
     *
     * @param acquiringSafe if true, acquires the safe commit; otherwise the last commit
     */
    default GatedCloseable<CatalogSnapshot> acquireCommittedSnapshot(boolean acquiringSafe) {
        throw new UnsupportedOperationException("acquireCommittedSnapshot not supported by this policy");
    }
}
