/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;

/**
 * Lifecycle operations for the indexer including flush, refresh, merge, and throttling.
 * Manages the lifecycle of indexing operations from buffering to persistence and searchability.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexerLifecycleOperations {

    /**
     * Makes recently indexed documents searchable by creating new segments from buffered data.
     * This operation does not guarantee durability; use flush for persistence.
     *
     * @param source description of what triggered the refresh (e.g., "api", "schedule")
     * @throws EngineException if the refresh operation fails
     */
    void refresh(String source) throws EngineException;

    /**
     * Commits buffered data to disk, ensuring durability of indexed documents.
     *
     * @param force if true, forces a flush even if not needed
     * @param waitIfOngoing if true, waits for any ongoing flush to complete; if false, returns immediately
     * @throws EngineException if the flush operation fails
     */
    void flush(boolean force, boolean waitIfOngoing);

    /**
     * Commits buffered data to disk with default behavior (non-forced, waits if ongoing).
     *
     * @throws EngineException if the flush operation fails
     */
    void flush();

    /**
     * Checks whether a periodic flush should be triggered based on translog size and age thresholds.
     *
     * @return true if a flush should be performed, false otherwise
     */
    boolean shouldPeriodicallyFlush();

    /**
     * Writes the active indexing buffer to disk without committing.
     * Used to free memory while keeping data in the translog.
     *
     * @throws EngineException if writing the buffer fails
     */
    void writeIndexingBuffer() throws EngineException;

    /**
     * Performs a force merge operation to reduce the number of segments.
     * This is an expensive operation that should be used sparingly.
     *
     * @param flush whether to flush after the merge
     * @param maxNumSegments target number of segments (1 for full merge)
     * @param onlyExpungeDeletes if true, only merges segments with deletions
     * @param upgrade if true, upgrades segments to the current format
     * @param upgradeOnlyAncientSegments if true, only upgrades old format segments
     * @param forceMergeUUID unique identifier for this force merge operation
     * @throws EngineException if the merge operation fails
     * @throws IOException if an I/O error occurs during merging
     */
    void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException;

    /**
     * Returns the amount of RAM currently used by the indexing buffer.
     *
     * @return RAM usage in bytes
     */
    long getIndexBufferRAMBytesUsed();

    /**
     * Activates indexing throttling to limit indexing to one thread.
     * Used to reduce load during recovery or when the system is under pressure.
     * Must be matched by a call to {@link #deactivateThrottling()}.
     */
    void activateThrottling();

    /**
     * Deactivates indexing throttling, allowing normal indexing concurrency.
     * Must be called after {@link #activateThrottling()}.
     */
    void deactivateThrottling();

    /**
     * Checks whether indexing is currently throttled.
     *
     * @return true if throttling is active, false otherwise
     */
    boolean isThrottled();

    /**
     * Applies changes to translog and soft-deletes retention settings.
     *
     * @param translogRetentionAge maximum age of translog files to retain
     * @param translogRetentionSize maximum size of translog files to retain
     * @param softDeletesRetentionOps number of soft-deleted operations to retain
     */
    void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps);

    /**
     * Finalizes replication by applying catalog snapshot changes.
     * Used in segment replication to apply received segment information.
     *
     * @param catalogSnapshot the catalog snapshot containing segment metadata
     * @param shardPath the shard path where segments are located
     * @throws IOException if finalization fails
     */
    default void finalizeReplication(CatalogSnapshot catalogSnapshot, ShardPath shardPath) throws IOException {
        // No-op by default
    }

    /**
     * Checks whether a refresh is needed based on buffered operations.
     *
     * @return true if a refresh should be performed, false otherwise
     */
    boolean refreshNeeded();

    /**
     * Performs a refresh if needed based on internal heuristics.
     *
     * @param source description of what triggered the refresh check
     * @return true if a refresh was performed, false otherwise
     */
    boolean maybeRefresh(String source);

    /**
     * Prunes deleted documents if the deletion ratio exceeds configured thresholds.
     * This helps reclaim disk space by removing tombstones.
     */
    void maybePruneDeletes();

    /**
     * Verifies the engine is in a valid state before closing the index.
     * Ensures no ongoing operations that would prevent safe closure.
     *
     * @throws IllegalStateException if the engine is not in a valid state for closing
     */
    void verifyEngineBeforeIndexClosing() throws IllegalStateException;
}
