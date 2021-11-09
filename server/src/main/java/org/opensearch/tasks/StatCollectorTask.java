/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import java.util.HashMap;
import java.util.Map;

public class StatCollectorTask extends CancellableTask {
    private Map<String, Long> allStats;

    public StatCollectorTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
        allStats = new HashMap<>();
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }

    public Map<String, Long> getStats() {
        return allStats;
    }

    public void updateStat(TaskStatsType statsType, long value) {
        allStats.put(statsType.toString(), value);
    }
}
