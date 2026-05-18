/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.planner.dag.ExecutionTarget;

/**
 * {@link StageTask} variant dispatched to a remote data node via Arrow Flight.
 * The {@link ExecutionTarget} carries the routing key and the per-partition
 * fragment payload the data node needs to set up its input source.
 *
 * @opensearch.internal
 */
public final class ShardStageTask extends StageTask {

    private final ExecutionTarget target;

    public ShardStageTask(StageTaskId id, ExecutionTarget target) {
        super(id);
        this.target = target;
    }

    public ExecutionTarget target() {
        return target;
    }
}
