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
 * Identifies where and how a stage's fragment should execute on a single node.
 *
 * <p>{@link #node()} is the mandatory routing key — the Scheduler sends one
 * {@code FragmentExecutionRequest} per target to the target's node. Subclasses
 * carry the execution-specific context the data node needs to set up its input source.
 *
 * <p>TODO: add ShuffleFileExecutionTarget, ShuffleStreamExecutionTarget,
 * InMemoryExecutionTarget when shuffle aggregates, joins, and broadcast joins
 * are implemented.
 *
 * @opensearch.internal
 */
public abstract class ExecutionTarget {

    private final DiscoveryNode node;

    protected ExecutionTarget(DiscoveryNode node) {
        this.node = node;
    }

    /** The node this target executes on. Used by the Scheduler for request routing. */
    public DiscoveryNode node() {
        return node;
    }
}
