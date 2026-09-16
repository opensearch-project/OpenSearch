/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Policy describing how a writable primary shard sources sequence numbers and plans operations.
 * <p>
 * Implementations are consulted on the indexing hot path. They must be stateless and thread-safe.
 * <p>
 * A policy may plan an operation with non-primary rather than primary semantics, which is how a
 * shard whose sequence numbers are assigned upstream avoids re-evaluating a compare-and-swap that
 * the upstream authority has already resolved. Non-primary planning reads
 * {@link Engine#getMaxSeqNoOfUpdatesOrDeletes()}, and neither the policy nor the engine advances it
 * on the operation's behalf: whatever replays operations onto the primary must call
 * {@link Engine#advanceMaxSeqNoOfUpdatesOrDeletes(long)} (normally through
 * {@code IndexShard#advanceMaxSeqNoOfUpdatesOrDeletes}) with the upstream authority's value before
 * applying each operation, exactly as the replica path does. If that marker is left behind, an
 * update to an existing document can be planned as an append-only insert, writing a second Lucene
 * document for the same {@code _id} instead of replacing the first.
 * <p>
 * <b>Core ships exactly one implementation:</b> {@link DefaultPrimaryOperationPolicy}, which is
 * core's own behavior and the fallback when no plugin supplies a policy.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface PrimaryOperationPolicy {

    /**
     * Whether a primary shard accepts sequence numbers pre-assigned by an upstream authority
     * instead of generating them locally.
     * <p>
     * Returning {@code true} declares that this shard is not the authority for its sequence-number
     * space, which carries a second consequence beyond sequence-number assignment:
     * {@link Engine#fillSeqNoGaps(long)} becomes a no-op, because recording promotion no-ops could
     * collide with sequence numbers the upstream authority has not yet replicated to this shard. A
     * shard that generates its own sequence numbers must fill gaps, since that is what lets its
     * local checkpoint advance past an operation that never completed.
     * <p>
     * Only an operation that reaches the engine with its sequence number already set can satisfy
     * this. {@code IndexShard#applyTranslogOperation} with {@link Engine.Operation.Origin#PRIMARY}
     * is the entry point that preserves one; the primary entry points used for client writes always
     * assign the sequence number locally.
     *
     * @return {@code true} if pre-assigned sequence numbers are expected on the primary
     */
    boolean acceptsPreAssignedSeqNos();

    /**
     * Plans an index operation that arrived on the primary.
     * <p>
     * The engine handles non-primary origins itself; this method is consulted only for
     * {@link Engine.Operation.Origin#PRIMARY}.
     * <p>
     * Planning with non-primary semantics carries the
     * {@code max_seq_no_of_updates_or_deletes} precondition described on this interface.
     *
     * @param planner the engine's index-operation planner
     * @param index the operation to plan
     * @return the plan the engine should execute
     * @throws IOException if planning performs I/O that fails
     */
    IndexingStrategy planIndex(OperationStrategyPlanner<Engine.Index, IndexingStrategy> planner, Engine.Index index) throws IOException;

    /**
     * Plans a delete operation that arrived on the primary.
     * <p>
     * The engine handles non-primary origins itself; this method is consulted only for
     * {@link Engine.Operation.Origin#PRIMARY}.
     * <p>
     * Planning with non-primary semantics carries the
     * {@code max_seq_no_of_updates_or_deletes} precondition described on this interface.
     *
     * @param planner the engine's delete-operation planner
     * @param delete the operation to plan
     * @return the plan the engine should execute
     * @throws IOException if planning performs I/O that fails
     */
    DeletionStrategy planDelete(OperationStrategyPlanner<Engine.Delete, DeletionStrategy> planner, Engine.Delete delete) throws IOException;
}
