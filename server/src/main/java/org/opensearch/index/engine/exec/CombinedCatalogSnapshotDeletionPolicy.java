/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * A {@link CatalogSnapshotDeletionPolicy} that coordinates between CatalogSnapshot commits
 * and the retention of translog generation files, making sure that all translog files needed
 * to recover from the safe commit are not deleted.
 * <p>
 * This policy will delete committed snapshots whose max sequence number is at most the current
 * global checkpoint, except the snapshot which has the highest max sequence number among those.
 * <p>
 * Mirrors {@link org.opensearch.index.engine.CombinedDeletionPolicy} for the pluggable engine world.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CombinedCatalogSnapshotDeletionPolicy implements CatalogSnapshotDeletionPolicy {

    private final Logger logger;
    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final LongSupplier globalCheckpointSupplier;
    private final Map<CatalogSnapshot, Integer> snapshottedCommits;

    private volatile CatalogSnapshot safeCommit;
    private volatile long maxSeqNoOfNextSafeCommit;
    private volatile CatalogSnapshot lastCommit;
    private volatile SafeCommitInfo safeCommitInfo = SafeCommitInfo.EMPTY;

    public CombinedCatalogSnapshotDeletionPolicy(
        Logger logger,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier
    ) {
        this.logger = logger;
        this.translogDeletionPolicy = translogDeletionPolicy;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.snapshottedCommits = new HashMap<>();
    }

    @Override
    public List<CatalogSnapshot> onInit(List<CatalogSnapshot> commits) throws IOException {
        assert commits.isEmpty() == false : "index is opened, but we have no commits";
        List<CatalogSnapshot> toDelete = onCommit(commits);
        if (safeCommit != commits.getLast()) {
            throw new IllegalStateException(
                "Engine is opened, but the last commit isn't safe. Global checkpoint ["
                    + globalCheckpointSupplier.getAsLong()
                    + "], safe commit [gen="
                    + safeCommit.getGeneration()
                    + ", maxSeqNo="
                    + safeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)
                    + "], last commit [gen="
                    + commits.getLast().getGeneration()
                    + ", maxSeqNo="
                    + commits.getLast().getUserData().get(SequenceNumbers.MAX_SEQ_NO)
                    + "]"
            );
        }
        return toDelete;
    }

    @Override
    public synchronized List<CatalogSnapshot> onCommit(List<CatalogSnapshot> commits) throws IOException {
        final int keptPosition = indexOfKeptCommits(commits, globalCheckpointSupplier.getAsLong());
        this.safeCommitInfo = SafeCommitInfo.EMPTY;
        this.lastCommit = commits.getLast();
        this.safeCommit = commits.get(keptPosition);

        List<CatalogSnapshot> toDelete = new ArrayList<>();
        for (int i = 0; i < keptPosition; i++) {
            if (snapshottedCommits.containsKey(commits.get(i)) == false) {
                logger.debug("Delete catalog snapshot commit [{}]", commitDescription(commits.get(i)));
                toDelete.add(commits.get(i));
            }
        }

        updateRetentionPolicy();

        if (keptPosition == commits.size() - 1) {
            this.maxSeqNoOfNextSafeCommit = Long.MAX_VALUE;
        } else {
            this.maxSeqNoOfNextSafeCommit = Long.parseLong(commits.get(keptPosition + 1).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
        }

        final CatalogSnapshot currentSafeCommit = this.safeCommit;
        safeCommitInfo = new SafeCommitInfo(
            Long.parseLong(currentSafeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
            getDocCountOfCommit(currentSafeCommit)
        );

        return toDelete;
    }

    private void updateRetentionPolicy() throws IOException {
        assert Thread.holdsLock(this);
        logger.debug("Safe commit [{}], last commit [{}]", commitDescription(safeCommit), commitDescription(lastCommit));
        final long localCheckpointOfSafeCommit = Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        translogDeletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
    }

    /**
     * Acquire the most recent safe or last committed snapshot.
     * The snapshot won't be deleted until the returned {@link GatedCloseable} is closed.
     *
     * @param acquiringSafe if true, acquires the safe commit; otherwise the last commit
     */
    @Override
    public synchronized GatedCloseable<CatalogSnapshot> acquireCommittedSnapshot(boolean acquiringSafe) {
        assert safeCommit != null : "Safe commit is not initialized yet";
        assert lastCommit != null : "Last commit is not initialized yet";
        final CatalogSnapshot snapshotting = acquiringSafe ? safeCommit : lastCommit;
        snapshottedCommits.merge(snapshotting, 1, Integer::sum);
        return new GatedCloseable<>(snapshotting, () -> releaseCommit(snapshotting));
    }

    private synchronized void releaseCommit(CatalogSnapshot snapshot) {
        assert snapshottedCommits.containsKey(snapshot) : "Release non-snapshotted commit;"
            + "snapshotted commits ["
            + snapshottedCommits
            + "], releasing ["
            + snapshot.getGeneration()
            + "]";
        final int refCount = snapshottedCommits.merge(snapshot, -1, Integer::sum);
        assert refCount >= 0 : "Number of snapshots can not be negative [" + refCount + "]";
        if (refCount == 0) {
            snapshottedCommits.remove(snapshot);
        }
    }

    public synchronized boolean hasSnapshottedCommits() {
        return snapshottedCommits.isEmpty() == false;
    }

    public boolean hasUnreferencedCommits() {
        return maxSeqNoOfNextSafeCommit <= globalCheckpointSupplier.getAsLong();
    }

    public CatalogSnapshot getSafeCommit() {
        return safeCommit;
    }

    public CatalogSnapshot getLastCommit() {
        return lastCommit;
    }

    /**
     * Returns the total document count of a committed catalog snapshot by summing
     * the row counts across all segments. Validates that all data formats within a segment
     * report the same row count.
     *
     * @throws IllegalStateException if a segment has inconsistent row counts across data formats
     */
    public static int getDocCountOfCommit(CatalogSnapshot snapshot) {
        long totalDocs = 0;
        for (Segment segment : snapshot.getSegments()) {
            long segmentRows = -1;
            for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                long numRows = entry.getValue().numRows();
                if (segmentRows == -1) {
                    segmentRows = numRows;
                } else if (segmentRows != numRows) {
                    throw new IllegalStateException(
                        "Segment [gen="
                            + segment.generation()
                            + "] has inconsistent row counts across data formats: "
                            + segmentRows
                            + " vs "
                            + numRows
                            + " (format="
                            + entry.getKey()
                            + ")"
                    );
                }
            }
            if (segmentRows > 0) {
                totalDocs += segmentRows;
            }
        }
        return Math.toIntExact(totalDocs);
    }

    /**
     * Returns information about the safe commit, for making decisions about recoveries.
     */
    public SafeCommitInfo getSafeCommitInfo() {
        return safeCommitInfo;
    }

    /**
     * Finds the safe commit point from the given list of commits and global checkpoint.
     *
     * @param commits the list of committed catalog snapshots, sorted by age (oldest first)
     * @param globalCheckpoint the persisted global checkpoint
     * @return the safe commit
     */
    public static CatalogSnapshot findSafeCommitPoint(List<CatalogSnapshot> commits, long globalCheckpoint) throws IOException {
        if (commits.isEmpty()) {
            throw new IllegalArgumentException("Commit list must not be empty");
        }
        final int keptPosition = indexOfKeptCommits(commits, globalCheckpoint);
        return commits.get(keptPosition);
    }

    /**
     * Returns a description for a given {@link CatalogSnapshot}. For logging and debugging only.
     */
    public static String commitDescription(CatalogSnapshot snapshot) {
        return String.format(Locale.ROOT, "CommitPoint{gen[%d], userData%s}", snapshot.getGeneration(), snapshot.getUserData());
    }

    /**
     * Find the highest index position of a safe commit whose max sequence number
     * is not greater than the global checkpoint. Commits with different translog UUID
     * are filtered out as they don't belong to this engine.
     */
    private static int indexOfKeptCommits(List<CatalogSnapshot> commits, long globalCheckpoint) throws IOException {
        final String expectedTranslogUUID = commits.getLast().getUserData().get(Translog.TRANSLOG_UUID_KEY);
        assert expectedTranslogUUID != null : "last commit must have a translog UUID";

        for (int i = commits.size() - 1; i >= 0; i--) {
            final Map<String, String> userData = commits.get(i).getUserData();
            if (expectedTranslogUUID.equals(userData.get(Translog.TRANSLOG_UUID_KEY)) == false) {
                return i + 1;
            }
            final String maxSeqNoStr = userData.get(SequenceNumbers.MAX_SEQ_NO);
            assert maxSeqNoStr != null : "commit [gen=" + commits.get(i).getGeneration() + "] missing " + SequenceNumbers.MAX_SEQ_NO;
            final long maxSeqNo = Long.parseLong(maxSeqNoStr);
            if (maxSeqNo <= globalCheckpoint) {
                return i;
            }
        }
        return 0;
    }
}
