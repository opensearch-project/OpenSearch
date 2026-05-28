/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.index.shard.ShardId;

/**
 * Execution target for a data node shard scan. The data node registers the shard
 * as a named table source before executing the fragment.
 *
 * <p>{@link #ordinal()} is a per-shard sequence number assigned by the
 * {@link TargetResolver} that produced this target. It is the index of this
 * target within the resolver's output list and is stable for the lifetime of
 * the resolved list. Generic per-shard property — consumed today by QTF
 * (Late Materialization stamps it as the {@code ___ugsi} column on every batch
 * the shard produces) but not LM-specific.
 *
 * @opensearch.internal
 */
public final class ShardExecutionTarget extends ExecutionTarget {

    private final ShardId shardId;
    private final int ordinal;

    public ShardExecutionTarget(DiscoveryNode node, ShardId shardId, int ordinal) {
        super(node);
        this.shardId = shardId;
        this.ordinal = ordinal;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int ordinal() {
        return ordinal;
    }
}
