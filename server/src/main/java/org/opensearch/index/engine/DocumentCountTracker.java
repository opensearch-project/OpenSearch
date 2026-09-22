/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks in-flight document counts to prevent exceeding the maximum document limit per shard.
 * <p>
 * This class is shared between {@link InternalEngine} (Lucene-based) and
 * {@link DataFormatAwareEngine} (pluggable data format). It guards against a race where
 * multiple concurrent writes pass the document count check but haven't yet been committed —
 * without tracking, the underlying storage could reject the write after a sequence number
 * has already been assigned, creating gaps.
 *
 * @opensearch.internal
 */
class DocumentCountTracker {

    /**
     * If multiple writes passed {@link DocumentCountTracker#tryAcquireInFlightDocs(Engine.Operation, int)} but they haven't adjusted
     * {@link IndexWriter#getPendingNumDocs()} yet, then IndexWriter can fail with too many documents. In this case, we have to fail
     * the engine because we already generated sequence numbers for write operations; otherwise we will have gaps in sequence numbers.
     * To avoid this, we keep track the number of documents that are being added to IndexWriter, and account it in
     * {@link DocumentCountTracker#tryAcquireInFlightDocs(Engine.Operation, int)}. Although we can double count some inFlight documents in IW and Engine,
     * this shouldn't be an issue because it happens for a short window and we adjust the inFlightDocCount once an indexing is completed.
     */
    private final AtomicLong inFlightDocCount = new AtomicLong();

    private final CheckedSupplier<Long, IOException> indexedDocs;
    private final long docsAllowed;
    private final ShardId shardId;

    /**
     * @param shardId     the shard this tracker belongs to (for error messages)
     * @param indexedDocs supplier returning the current committed + pending doc count
     * @param docsAllowed maximum number of documents allowed in this shard
     */
    DocumentCountTracker(ShardId shardId, CheckedSupplier<Long, IOException> indexedDocs, long docsAllowed) {
        this.shardId = shardId;
        this.indexedDocs = indexedDocs;
        this.docsAllowed = docsAllowed;
    }

    /**
     * Attempts to reserve {@code addingDocs} document slots. If the total (in-flight + indexed)
     * would exceed the limit, the reservation is rolled back and an exception is returned.
     *
     * @param operation  the engine operation (must be PRIMARY with unassigned seq no)
     * @param addingDocs number of documents being added
     * @return {@code null} if acquired successfully, or an {@link IllegalArgumentException} if the limit would be exceeded
     */
    Exception tryAcquireInFlightDocs(Engine.Operation operation, int addingDocs) {
        assert operation.origin() == Engine.Operation.Origin.PRIMARY : operation;
        assert operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO : operation;
        assert addingDocs > 0 : addingDocs;
        long totalDocs;
        try {
            totalDocs = inFlightDocCount.addAndGet(addingDocs) + indexedDocs.get();
        } catch (IOException e) {
            releaseInFlightDocs(addingDocs);
            return e;
        }
        if (totalDocs > docsAllowed) {
            releaseInFlightDocs(addingDocs);
            return new IllegalArgumentException(
                "Number of documents in shard " + shardId + " exceeds the limit of [" + docsAllowed + "] documents per shard"
            );
        } else {
            return null;
        }
    }

    /**
     * Releases previously acquired in-flight document slots after indexing completes or fails.
     *
     * @param numDocs number of document slots to release (must be non-negative)
     */
    void releaseInFlightDocs(int numDocs) {
        assert numDocs >= 0 : numDocs;
        final long newValue = inFlightDocCount.addAndGet(-numDocs);
        assert newValue >= 0 : "inFlightDocCount must not be negative [" + newValue + "]";
    }

    long getInFlightDocCount() {
        return inFlightDocCount.get();
    }
}
