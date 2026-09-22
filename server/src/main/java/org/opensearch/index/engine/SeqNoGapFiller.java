/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.translog.TranslogManager;

import java.io.IOException;

/**
 * Shared loop that fills gaps in a {@link LocalCheckpointTracker}'s seqNo space by issuing
 * NoOp operations until the local checkpoint catches up to the max seq no.
 *
 * <p>Used by primary-side engines after a recovery or primary-term bump to plug seqNos that
 * were assigned but never made it into a durable record (e.g., the previous primary crashed
 * before persisting). Without this step, the local checkpoint stays stuck below maxSeqNo and
 * blocks replication progress.
 *
 * <p>Each engine supplies its own {@link NoOpHandler} so the leaf operation can vary —
 * {@link InternalEngine} writes a soft-delete tombstone, {@link DataFormatAwareEngine} does
 * not (it has no tombstone concept). The loop, leap-frog over the processed checkpoint, and
 * post-fill translog sync are identical across both.
 *
 * <p>Caller must hold the engine's write lock for the duration.
 */
final class SeqNoGapFiller {

    private SeqNoGapFiller() {}

    @FunctionalInterface
    interface NoOpHandler {
        void apply(Engine.NoOp noOp) throws IOException;
    }

    /**
     * Issues NoOps for every seqNo gap below {@code maxSeqNo} via {@code handler}, then
     * fsyncs the translog.
     *
     * @return the number of NoOps written
     */
    static int fillGaps(LocalCheckpointTracker tracker, TranslogManager translogManager, long primaryTerm, NoOpHandler handler)
        throws IOException {
        final long localCheckpoint = tracker.getProcessedCheckpoint();
        final long maxSeqNo = tracker.getMaxSeqNo();
        int numNoOpsAdded = 0;
        for (long seqNo = localCheckpoint + 1; seqNo <= maxSeqNo; seqNo = tracker.getProcessedCheckpoint()
            + 1 /* leap-frog the local checkpoint */) {
            handler.apply(new Engine.NoOp(seqNo, primaryTerm, Engine.Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps"));
            numNoOpsAdded++;
            assert seqNo <= tracker.getProcessedCheckpoint() : "local checkpoint did not advance; was ["
                + seqNo
                + "], now ["
                + tracker.getProcessedCheckpoint()
                + "]";
        }
        translogManager.syncTranslog(); // persist noops associated with the advancement of the local checkpoint
        assert tracker.getPersistedCheckpoint() == maxSeqNo : "persisted local checkpoint did not advance to max seq no; is ["
            + tracker.getPersistedCheckpoint()
            + "], max seq no ["
            + maxSeqNo
            + "]";
        return numNoOpsAdded;
    }
}
