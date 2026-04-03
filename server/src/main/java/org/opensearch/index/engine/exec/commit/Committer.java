/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;

import java.io.Closeable;
import java.io.IOException;
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
 * Lifecycle: {@link #init(CommitterSettings)} is called once during engine construction,
 * {@link #commit(Map)} is called during flush, and {@link #close()} is called
 * during engine shutdown.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Committer extends Closeable {

    /**
     * Initializes the committer with the given settings.
     * Called once during engine construction before any indexing operations.
     *
     * @param settings initialization parameters (e.g., shard path, index settings)
     * @throws IOException if initialization fails
     */
    void init(CommitterSettings settings) throws IOException;

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
     * For Lucene-backed implementations, this is the commit userData from the last
     * {@code IndexWriter.commit()} call, which includes the serialized CatalogSnapshot.
     *
     * @return the last committed user data, or an empty map if no commit has occurred
     * @throws IOException if reading the commit data fails
     */
    Map<String, String> getLastCommittedData() throws IOException;

    /**
     * Returns statistics about the last commit point.
     * Includes generation, user data, commit ID, and document count.
     *
     * @return the commit stats, or null if no commit has occurred
     */
    CommitStats getCommitStats();

    /**
     * Returns information about the safe commit point for recovery decisions.
     * The safe commit is the most recent commit that is safe to recover from,
     * carrying the local checkpoint and document count.
     *
     * @return the safe commit info
     */
    SafeCommitInfo getSafeCommitInfo();
}
