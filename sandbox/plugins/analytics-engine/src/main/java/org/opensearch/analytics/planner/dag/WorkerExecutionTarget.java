/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.cluster.node.DiscoveryNode;

/**
 * Execution target for a hash-shuffle worker task. The worker stage fans out into
 * one task per shuffle partition; each task carries the {@code (node, partitionIndex)}
 * pair so the worker plan can drain only its partition's slice from the shuffle
 * buffer.
 *
 * @opensearch.internal
 */
public final class WorkerExecutionTarget extends ExecutionTarget {

    private final int partitionIndex;

    public WorkerExecutionTarget(DiscoveryNode node, int partitionIndex) {
        super(node);
        this.partitionIndex = partitionIndex;
    }

    public int partitionIndex() {
        return partitionIndex;
    }
}
