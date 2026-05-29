/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.FailAwareWeightedRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Nullable;
import org.opensearch.core.index.shard.ShardId;

/**
 * Execution target for a data node shard scan. The data node registers the shard
 * as a named table source before executing the fragment.
 *
 * <p>{@link #ordinal()} is a per-shard sequence number assigned by the
 * {@link TargetResolver} that produced this target. It is the index of this target
 * within the resolver's output list and is stable for the lifetime of the resolved
 * list — preserved across replica failover via {@link #nextCopy(Exception)}. Generic
 * per-shard property; consumed today by QTF (Late Materialization stamps it as the
 * {@code ___ugsi} column on every batch the shard produces).
 *
 * <p>Carries a {@link ShardIterator} pointing at the remaining replica copies for
 * the underlying {@link ShardId}; on dispatch failure, {@link #nextCopy(Exception)}
 * advances to the next copy on a different node, mirroring the search API's
 * {@code AbstractSearchAsyncAction.onShardFailure} → {@code FailAwareWeightedRouting.findNext}
 * flow. The iterator's {@code nextOrNull} state is mutated by {@code nextCopy()},
 * so each {@code ShardExecutionTarget} owns the remaining-copies state for the
 * retry chain it kicks off — copies aren't shared across tasks.
 *
 * @opensearch.internal
 */
public final class ShardExecutionTarget extends ExecutionTarget {

    private final ShardId shardId;
    private final int ordinal;
    @Nullable
    private final ShardIterator remainingCopies;
    @Nullable
    private final ClusterState clusterState;

    /**
     * Constructor for callers that don't track replica copies (tests, fragments where
     * routing isn't available). {@link #nextCopy(Exception)} on a target built this way
     * returns null, preserving the original "no failover" behavior.
     */
    public ShardExecutionTarget(DiscoveryNode node, ShardId shardId, int ordinal) {
        this(node, shardId, ordinal, null, null);
    }

    /**
     * Full constructor with replica-failover state. {@code remainingCopies} is the
     * post-initial-pop position of the {@link ShardIterator} from
     * {@code OperationRouting.searchShards} — {@link #nextCopy(Exception)} advances it on retry.
     */
    public ShardExecutionTarget(
        DiscoveryNode node,
        ShardId shardId,
        int ordinal,
        @Nullable ShardIterator remainingCopies,
        @Nullable ClusterState clusterState
    ) {
        super(node);
        this.shardId = shardId;
        this.ordinal = ordinal;
        this.remainingCopies = remainingCopies;
        this.clusterState = clusterState;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int ordinal() {
        return ordinal;
    }

    /**
     * Returns a target for the next viable replica copy of this shard, or {@code null} when
     * the iterator is exhausted (or when this target wasn't constructed with iterator state).
     * The returned target carries the same {@link #ordinal()} — the per-shard slot identifier
     * is stable across replica failover.
     *
     * <p>Delegates to {@link FailAwareWeightedRouting#findNext}, honoring weighted-routing
     * skip rules: copies on weighed-away nodes are skipped, with fail-open semantics
     * applied when the iterator would otherwise leave no candidates. The cause is forwarded
     * to the fail-open decision (see {@code FailAwareWeightedRouting.canFailOpen}).
     */
    @Nullable
    public ShardExecutionTarget nextCopy(@Nullable Exception cause) {
        if (remainingCopies == null || clusterState == null) {
            return null;
        }
        ShardRouting next = FailAwareWeightedRouting.getInstance().findNext(remainingCopies, clusterState, cause, () -> {});
        if (next == null) {
            return null;
        }
        DiscoveryNode nextNode = clusterState.nodes().get(next.currentNodeId());
        if (nextNode == null) {
            return null;
        }
        return new ShardExecutionTarget(nextNode, next.shardId(), ordinal, remainingCopies, clusterState);
    }
}
