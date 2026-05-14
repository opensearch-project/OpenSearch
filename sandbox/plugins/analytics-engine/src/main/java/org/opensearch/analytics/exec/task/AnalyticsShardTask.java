/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

import java.util.Map;

/**
 * Data-node shard task representing a single shard fragment execution.
 * Analogous to {@link org.opensearch.action.search.SearchShardTask}.
 * Cancelling this task does not cascade to children.
 *
 * @opensearch.internal
 */
public class AnalyticsShardTask extends CancellableTask {

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
