/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.CommitFileManager;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Abstraction for durably committing data to a backing store.
 * <p>
 * Implementations persist commit data (key-value pairs) so it can be recovered after a restart.
 * The canonical implementation stores the data as Lucene commit userData via
 * {@code IndexWriter.setLiveCommitData} + {@code IndexWriter.commit()}.
 * <p>
 * The caller is responsible for serializing any higher-level state (e.g., CatalogSnapshot)
 * into the commit data before calling {@link #commit}.
 * <p>
 * Implementations are constructed with {@link CommitterConfig} which provides the shard path,
 * index settings, and engine config needed to open the backing store. There is no separate
 * {@code init()} method — the constructor handles all initialization.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Committer extends CommitFileManager, Closeable {

    /**
     * Durably commits the given data to the backing store's commit metadata.
     * Called during the engine's flush path.
     *
     * @param commitData the key-value pairs to persist as commit metadata
     * @throws IOException if the commit fails
     */
    void commit(Map<String, String> commitData) throws IOException;

    /**
     * Returns the user data from the last successful commit.
     *
     * @return the last committed user data, or an empty map if no commit has occurred
     * @throws IOException if reading the commit data fails
     */
    Map<String, String> getLastCommittedData() throws IOException;

    /**
     * Returns statistics about the last commit point.
     *
     * @return the commit stats, or null if no commit has occurred
     */
    CommitStats getCommitStats();

    /**
     * Returns information about the safe commit point for recovery decisions.
     *
     * @return the safe commit info
     */
    SafeCommitInfo getSafeCommitInfo();

    /**
     * Discovers all persisted committed catalog snapshots from the backing store.
     * Returns them ordered oldest-first (by generation).
     * Returns empty list if no catalog snapshots have been committed.
     *
     * @return the list of committed catalog snapshots
     * @throws IOException if reading commits fails
     */
    List<CatalogSnapshot> listCommittedSnapshots() throws IOException;

    /**
     * Returns the tragic exception that has occurred in the backing store, if any.
     *
     * @return the tragic exception, or null if none
     */
    @Nullable
    default Exception getTragicException() {
        return null;
    }
}
