/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.List;

/**
 * Abstract {@link Committer} that enforces safe commit trimming during construction.
 * <p>
 * When a {@link CatalogSnapshotDeletionPolicy} is present in the {@link CommitterConfig},
 * the constructor discovers existing commits via {@link #discoverCommits}, finds the safe
 * commit, and rewrites at that point if the last commit is unsafe. This guarantees the
 * backing store is in a safe state before the subclass opens it.
 * <p>
 * Subclass constructors must call {@code super(config)} as their first statement.
 * The abstract methods receive {@link CommitterConfig} as a parameter because subclass
 * instance fields are not yet initialized when they are called.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class SafeBootstrapCommitter implements Committer {

    /**
     * Constructs the committer, trimming unsafe commits if a deletion policy is configured.
     *
     * @param config the committer configuration
     * @throws IOException if commit discovery or rewriting fails
     */
    protected SafeBootstrapCommitter(CommitterConfig config) throws IOException {
        CatalogSnapshotDeletionPolicy policy = config.deletionPolicy()
            .orElseThrow(() -> new IllegalArgumentException("SafeBootstrapCommitter requires a CatalogSnapshotDeletionPolicy in config"));
        List<CatalogSnapshot> commits = discoverCommits(config);
        if (commits == null) {
            throw new IllegalStateException("discoverCommits must not return null");
        }
        if (commits.isEmpty() == false) { // should it be ever empty?
            CatalogSnapshot safe = policy.findSafeCommit(commits);
            if (commits.size() > 1) {
                rewriteAtSafeCommit(config, commits, safe);
            }
        }
    }

    /**
     * Discover committed catalog snapshots from the backing store.
     * Called from the {@code SafeBootstrapCommitter} constructor — subclass instance fields
     * are NOT yet initialized. Use only the provided config.
     *
     * @param config the committer configuration
     * @return committed snapshots ordered oldest-first, or empty list if none
     * @throws IOException if reading commits fails
     */
    protected abstract List<CatalogSnapshot> discoverCommits(CommitterConfig config) throws IOException;

    /**
     * Rewrite the backing store at the safe commit point, discarding unsafe commits.
     * Called from the {@code SafeBootstrapCommitter} constructor — subclass instance fields
     * are NOT yet initialized. Use only the provided config.
     *
     * @param config the committer configuration
     * @param commits all discovered commits
     * @param safeCommit the safe commit to rewrite at
     * @throws IOException if rewriting fails
     */
    protected abstract void rewriteAtSafeCommit(CommitterConfig config, List<CatalogSnapshot> commits, CatalogSnapshot safeCommit)
        throws IOException;
}
