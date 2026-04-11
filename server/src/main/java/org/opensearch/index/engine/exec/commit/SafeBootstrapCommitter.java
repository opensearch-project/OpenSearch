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
import org.opensearch.index.store.Store;

import java.io.IOException;

/**
 * Abstract {@link Committer} that enforces safe commit trimming during construction.
 * <p>
 * The constructor validates that the {@link CommitterConfig} contains a non-null
 * {@link Store} and {@link CatalogSnapshotDeletionPolicy}, then delegates to
 * {@link #discoverAndTrimUnsafeCommits} which discovers existing commits, finds the
 * safe commit, and rewrites at that point if the last commit is unsafe. This guarantees
 * the backing store is in a safe state before the subclass opens it.
 * <p>
 * Subclass constructors must call {@code super(config)} as their first statement.
 * The abstract method receives {@link Store} and {@link CatalogSnapshotDeletionPolicy}
 * as parameters because subclass instance fields are not yet initialized when it is called.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class SafeBootstrapCommitter implements Committer {

    /**
     * Constructs the committer, validating config and trimming unsafe commits.
     *
     * @param config the committer configuration
     * @throws IOException if commit discovery or rewriting fails
     */
    protected SafeBootstrapCommitter(CommitterConfig config) throws IOException {
        Store store = config.store();
        CatalogSnapshotDeletionPolicy policy = config.deletionPolicy();
        if (store == null) {
            throw new IllegalArgumentException("SafeBootstrapCommitter requires a non-null Store in config");
        }
        if (policy == null) {
            throw new IllegalArgumentException("SafeBootstrapCommitter requires a non-null CatalogSnapshotDeletionPolicy in config");
        }
        discoverAndTrimUnsafeCommits(store, policy);
    }

    /**
     * Discover committed catalog snapshots from the backing store and, if the last
     * commit is unsafe, rewrite at the safe commit point discarding unsafe commits.
     * <p>
     * Called from the {@code SafeBootstrapCommitter} constructor — subclass instance fields
     * are NOT yet initialized. Use only the provided parameters.
     *
     * @param store  the shard's store providing the Lucene directory
     * @param policy the deletion policy used to find the safe commit
     * @throws IOException if reading or rewriting commits fails
     */
    protected abstract void discoverAndTrimUnsafeCommits(Store store, CatalogSnapshotDeletionPolicy policy) throws IOException;
}
