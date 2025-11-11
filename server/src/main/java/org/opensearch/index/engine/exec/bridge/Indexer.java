/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@PublicApi(since = "1.0.0")
public interface Indexer {

    Engine.IndexResult index(Engine.Index index) throws IOException;

    Engine.DeleteResult delete(Engine.Delete delete) throws IOException;

    Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException;

    /**
     * Counts the number of history operations in the given sequence number range
     * @param source       source of the request
     * @param fromSeqNo    from sequence number; included
     * @param toSeqNumber  to sequence number; included
     * @return number of history operations
     */
    int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException;

    boolean hasCompleteOperationHistory(String reason, long startingSeqNo);

    long getIndexBufferRAMBytesUsed();

    List<Segment> segments(boolean verbose);

    /**
     * Returns the maximum auto_id_timestamp of all append-only index requests have been processed by this engine
     * or the auto_id_timestamp received from its primary shard via {@link #updateMaxUnsafeAutoIdTimestamp(long)}.
     * Notes this method returns the auto_id_timestamp of all append-only requests, not max_unsafe_auto_id_timestamp.
     */
    long getMaxSeenAutoIdTimestamp();

    /**
     * Forces this engine to advance its max_unsafe_auto_id_timestamp marker to at least the given timestamp.
     * The engine will disable optimization for all append-only whose timestamp at most {@code newTimestamp}.
     */
    void updateMaxUnsafeAutoIdTimestamp(long newTimestamp);

    /**
     * Returns the maximum sequence number of either update or delete operations have been processed in this engine
     * or the sequence number from {@link #advanceMaxSeqNoOfUpdatesOrDeletes(long)}. An index request is considered
     * as an update operation if it overwrites the existing documents in the index with the same document id.
     * <p>
     * A note on the optimization using max_seq_no_of_updates_or_deletes:
     * For each operation O, the key invariants are:
     * <ol>
     *     <li> I1: There is no operation on docID(O) with seqno that is {@literal > MSU(O) and < seqno(O)} </li>
     *     <li> I2: If {@literal MSU(O) < seqno(O)} then docID(O) did not exist when O was applied; more precisely, if there is any O'
     *              with {@literal seqno(O') < seqno(O) and docID(O') = docID(O)} then the one with the greatest seqno is a delete.</li>
     * </ol>
     * <p>
     * When a receiving shard (either a replica or a follower) receives an operation O, it must first ensure its own MSU at least MSU(O),
     * and then compares its MSU to its local checkpoint (LCP). If {@literal LCP < MSU} then there's a gap: there may be some operations
     * that act on docID(O) about which we do not yet know, so we cannot perform an add. Note this also covers the case where a future
     * operation O' with {@literal seqNo(O') > seqNo(O) and docId(O') = docID(O)} is processed before O. In that case MSU(O') is at least
     * seqno(O') and this means {@literal MSU >= seqNo(O') > seqNo(O) > LCP} (because O wasn't processed yet).
     * <p>
     * However, if {@literal MSU <= LCP} then there is no gap: we have processed every {@literal operation <= LCP}, and no operation O'
     * with {@literal seqno(O') > LCP and seqno(O') < seqno(O) also has docID(O') = docID(O)}, because such an operation would have
     * {@literal seqno(O') > LCP >= MSU >= MSU(O)} which contradicts the first invariant. Furthermore in this case we immediately know
     * that docID(O) has been deleted (or never existed) without needing to check index for the following reason. If there's no earlier
     * operation on docID(O) then this is clear, so suppose instead that the preceding operation on docID(O) is O':
     * 1. The first invariant above tells us that {@literal seqno(O') <= MSU(O) <= LCP} so we have already applied O' to the index.
     * 2. Also {@literal MSU(O) <= MSU <= LCP < seqno(O)} (we discard O if {@literal seqno(O) <= LCP}) so the second invariant applies,
     *    meaning that the O' was a delete.
     * <p>
     * Therefore, if {@literal MSU <= LCP < seqno(O)} we know that O can safely be optimized with and added to the index with addDocument.
     * Moreover, operations that are optimized using the MSU optimization must not be processed twice as this will create duplicates
     * in the index. To avoid this we check the local checkpoint tracker to see if an operation was already processed.
     *
     * @see #advanceMaxSeqNoOfUpdatesOrDeletes(long)
     */
    long getMaxSeqNoOfUpdatesOrDeletes();

    /**
     * A replica shard receives a new max_seq_no_of_updates from its primary shard, then calls this method
     * to advance this marker to at least the given sequence number.
     */
    void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary);

    int fillSeqNoGaps(long primaryTerm) throws IOException;

    // File format methods follow below
    void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException;

    void writeIndexingBuffer() throws EngineException;

    void refresh(String source) throws EngineException;

    void flush(boolean force, boolean waitIfOngoing) throws EngineException;

    SafeCommitInfo getSafeCommitInfo();

    // Translog methods follow below
    TranslogManager translogManager();

    Closeable acquireHistoryRetentionLock();

    Translog.Snapshot newChangesSnapshot(String source, long fromSeqNo, long toSeqNo, boolean requiredFullRange, boolean accurateCount)
        throws IOException;

    String getHistoryUUID();

    /**
     * Whether we should treat any document failure as tragic error.
     * If we hit any failure while processing an indexing on a replica, we should treat that error as tragic and fail the engine.
     * However, we prefer to fail a request individually (instead of a shard) if we hit a document failure on the primary.
     */
    default boolean treatDocumentFailureAsTragicError(Engine.Index index) {
        // TODO: can we enable this check for all origins except primary on the leader?
        return index.origin() == Engine.Operation.Origin.REPLICA || index.origin() == Engine.Operation.Origin.PEER_RECOVERY
            || index.origin() == Engine.Operation.Origin.LOCAL_RESET;
    }

    default boolean assertIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (origin == Engine.Operation.Origin.PRIMARY) {
            assert assertPrimaryIncomingSequenceNumber(origin, seqNo);
        } else {
            // sequence number should be set when operation origin is not primary
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    default boolean assertPrimaryIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        // sequence number should not be set when operation origin is primary
        assert
            seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    /**
     * the status of the current doc version in engine, compared to the version in an incoming
     * operation
     */
    enum OpVsEngineDocStatus {
        /** the op is more recent than the one that last modified the doc found in engine*/
        OP_NEWER,
        /** the op is older or the same as the one that last modified the doc found in engine*/
        OP_STALE_OR_EQUAL,
        /** no doc was found in engine */
        DOC_NOT_FOUND
    }
}
