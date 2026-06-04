/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Abstract {@link Committer} that enforces safe commit trimming during construction.
 * <p>
 * The constructor validates that the {@link CommitterConfig} contains a non-null
 * {@link EngineConfig} with a valid {@link Store}, then delegates to
 * {@link #discoverAndTrimUnsafeCommits} which discovers existing commits, reads the
 * persisted global checkpoint from the translog checkpoint file, finds the safe commit,
 * and rewrites at that point if the last commit is unsafe. This guarantees the backing
 * store is in a safe state before the subclass opens it. Currently, it is coupled with the Store
 *
 * TODO - Decouple the store impl from here and rely on Committer provided static methods
 * <p>
 * Subclass constructors must call {@code super(config)} as their first statement.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class SafeBootstrapCommitter implements Committer {

    /**
     * Constructs the committer, validating config and trimming unsafe commits.
     * If translog config is absent, safe bootstrap trimming is skipped.
     *
     * @param config the committer configuration
     * @throws IOException if commit discovery or rewriting fails
     */
    protected SafeBootstrapCommitter(CommitterConfig config) throws IOException {
        EngineConfig engineConfig = config.engineConfig();
        if (engineConfig == null || engineConfig.getStore() == null) {
            throw new IllegalArgumentException("SafeBootstrapCommitter requires a non-null EngineConfig with a valid Store");
        }
        if (engineConfig.getTranslogConfig() == null || engineConfig.getTranslogConfig().getTranslogPath() == null) {
            throw new IllegalArgumentException("SafeBootstrapCommitter requires a non-null TranslogConfig with a valid translog path");
        }
        discoverAndTrimUnsafeCommits(engineConfig.getStore(), engineConfig.getTranslogConfig().getTranslogPath());
    }

    /**
     * Discover committed catalog snapshots from the backing store and, if the last
     * commit is unsafe, rewrite at the safe commit point discarding unsafe commits.
     * <p>
     * Implementations should read the persisted global checkpoint from the translog
     * checkpoint file via {@code Translog.readGlobalCheckpoint(translogPath, translogUUID)}
     * and use {@code CombinedCatalogSnapshotDeletionPolicy.findSafeCommitPoint(commits, gcp)}
     * to identify the safe commit.
     * <p>
     * Called from the {@code SafeBootstrapCommitter} constructor — subclass instance fields
     * are NOT yet initialized. Use only the provided parameters.
     *
     * @param store        the shard's store providing the Lucene directory
     * @param translogPath path to the translog directory for reading the persisted global checkpoint
     * @throws IOException if reading or rewriting commits fails
     */
    protected abstract void discoverAndTrimUnsafeCommits(Store store, Path translogPath) throws IOException;
}
