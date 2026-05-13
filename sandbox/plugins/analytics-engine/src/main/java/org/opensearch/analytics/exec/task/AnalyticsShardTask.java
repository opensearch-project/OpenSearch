/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.tasks.TaskId;

import java.util.Map;

/**
 * Data-node shard task representing a single shard fragment execution.
 * <p>Extends {@link SearchShardTask} so that search backpressure and
 * {@code TaskResourceTrackingService} observe and track analytics shard
 * tasks alongside regular search shard tasks. The native-memory tracker
 * in {@code SearchBackpressureService} filters by {@code SearchShardTask},
 * so inheriting from it is the integration point that exposes per-query
 * DataFusion memory to cancellation.
 *
 * <p>Cancelling this task does not cascade to children (inherited behaviour
 * from {@link SearchShardTask}).
 *
 * @opensearch.internal
 */
public class AnalyticsShardTask extends SearchShardTask {

    public AnalyticsShardTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }

    @Override
    public boolean supportsResourceTracking() {
        return true;
    }
}
