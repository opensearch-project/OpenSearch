/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom {@link IndexDeletionPolicy} for the LuceneCommitter's IndexWriter.
 * <p>
 * Tracks CatalogSnapshot commits by their {@link CatalogSnapshot#CATALOG_SNAPSHOT_ID}.
 * The initial non-CatalogSnapshot commit (from {@code store.createEmpty()}) is deleted
 * via {@link IndexCommit#delete()} once the first CatalogSnapshot commit appears.
 * <p>
 * CatalogSnapshot commits are staged for deletion via {@link #purgeCommit(long)}, and the
 * actual {@link IndexCommit#delete()} happens inside the next {@link #onCommit} callback.
 * The caller triggers this by invoking {@link org.apache.lucene.index.IndexWriter#deleteUnusedFiles()},
 * which calls {@code revisitPolicy()} → {@code onCommit()} → {@code deleteCommits()}.
 * Lucene's internal IndexFileDeleter then handles cleanup of both the segments_N file and
 * any unreferenced segment files.
 * <p>
 * This requires {@link org.apache.lucene.index.NoMergePolicy} on the IndexWriter to ensure
 * Lucene's internal ref counts stay in sync with our
 * {@link org.opensearch.index.engine.exec.coord.IndexFileDeleter}.
 */
class LuceneCommitDeletionPolicy extends IndexDeletionPolicy {

    private final Map<Long, IndexCommit> trackedCommits = new ConcurrentHashMap<>();
    private final Set<Long> pendingDeletes = ConcurrentHashMap.newKeySet();
    private volatile boolean hasCSCommit;
    private volatile IndexCommit nonCatalogSnapshotCommit;

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        // Stash the non-CS commit (e.g., from store.createEmpty()) before delegating to onCommit,
        // which will delete it once a CS commit exists.
        for (IndexCommit commit : commits) {
            if (commit.getUserData().get(CatalogSnapshot.CATALOG_SNAPSHOT_ID) == null) {
                nonCatalogSnapshotCommit = commit;
            }
        }
        onCommit(commits);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        for (IndexCommit commit : commits) {
            Map<String, String> userData = commit.getUserData();
            String idStr = userData.get(CatalogSnapshot.CATALOG_SNAPSHOT_ID);
            if (idStr != null) {
                long id = Long.parseLong(idStr);
                trackedCommits.putIfAbsent(id, commit);
                hasCSCommit = true;
                if (pendingDeletes.remove(id)) {
                    commit.delete();
                    trackedCommits.remove(id);
                }
            }
        }
        // Delete the initial non-CS commit (from store.createEmpty()) once a CS commit exists,
        // since it is no longer needed for recovery.
        if (hasCSCommit && nonCatalogSnapshotCommit != null) {
            nonCatalogSnapshotCommit.delete();
            nonCatalogSnapshotCommit = null;
        }
    }

    /**
     * Stages the commit for the given CatalogSnapshot ID for deletion.
     * The actual {@link IndexCommit#delete()} happens in the next {@link #onCommit} callback,
     * triggered by {@link org.apache.lucene.index.IndexWriter#deleteUnusedFiles()}.
     *
     * @param snapshotId the CatalogSnapshot ID to purge
     */
    void purgeCommit(long snapshotId) {
        pendingDeletes.add(snapshotId);
    }
}
