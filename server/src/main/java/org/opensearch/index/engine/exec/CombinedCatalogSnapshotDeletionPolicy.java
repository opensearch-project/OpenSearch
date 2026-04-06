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
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        if (safeCommit != commits.get(commits.size() - 1)) {
            throw new IllegalStateException(
                "Engine is opened, but the last commit isn't safe. Global checkpoint ["
                    + globalCheckpointSupplier.getAsLong()
                    + "], safe commit [gen="
                    + safeCommit.getGeneration()
                    + ", maxSeqNo="
                    + safeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)
                    + "], last commit [gen="
                    + commits.get(commits.size() - 1).getGeneration()
                    + ", maxSeqNo="
                    + commits.get(commits.size() - 1).getUserData().get(SequenceNumbers.MAX_SEQ_NO)
                    + "]"
            );
        }
        return toDelete;
    }

    @Override
    public synchronized List<CatalogSnapshot> onCommit(List<CatalogSnapshot> commits) throws IOException {
        final int keptPosition = indexOfKeptCommits(commits, globalCheckpointSupplier.getAsLong());
        this.lastCommit = commits.get(commits.size() - 1);
        this.safeCommit = commits.get(keptPosition);

        List<CatalogSnapshot> toDelete = new ArrayList<>();
        for (int i = 0; i < keptPosition; i++) {
            if (snapshottedCommits.containsKey(commits.get(i)) == false) {
                logger.debug("Delete catalog snapshot commit [gen={}]", commits.get(i).getGeneration());
                toDelete.add(commits.get(i));
            }
        }

        updateRetentionPolicy();

        if (keptPosition == commits.size() - 1) {
            this.maxSeqNoOfNextSafeCommit = Long.MAX_VALUE;
        } else {
            this.maxSeqNoOfNextSafeCommit = Long.parseLong(commits.get(keptPosition + 1).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
        }

        return toDelete;
    }

    private void updateRetentionPolicy() throws IOException {
        assert Thread.holdsLock(this);
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
     * Find the highest index position of a safe commit whose max sequence number
     * is not greater than the global checkpoint. Commits with different translog UUID
     * are filtered out as they don't belong to this engine.
     */
    private static int indexOfKeptCommits(List<CatalogSnapshot> commits, long globalCheckpoint) throws IOException {
        final String expectedTranslogUUID = commits.get(commits.size() - 1).getUserData().get(Translog.TRANSLOG_UUID_KEY);
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
