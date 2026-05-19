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
 * @opensearch.internal
 */
public final class ShardExecutionTarget extends ExecutionTarget {

    private final ShardId shardId;

    public ShardExecutionTarget(DiscoveryNode node, ShardId shardId) {
        super(node);
        this.shardId = shardId;
    }

    public ShardId shardId() {
        return shardId;
    }
}
