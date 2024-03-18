/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class SearchShardTaskDetailsLogMessageTests extends OpenSearchSingleNodeTestCase {
    public void testTaskDetailsLogHasJsonFields() {
        SearchShardTask task = new SearchShardTask(
            0,
            "n/a",
            "n/a",
            "test",
            null,
            Collections.singletonMap(Task.X_OPAQUE_ID, "my_id"),
            () -> "test_metadata"
        );
        SearchShardTaskDetailsLogMessage p = new SearchShardTaskDetailsLogMessage(task);

        assertThat(p.getValueFor("taskId"), equalTo("0"));
        assertThat(p.getValueFor("type"), equalTo("n/a"));
        assertThat(p.getValueFor("action"), equalTo("n/a"));
        assertThat(p.getValueFor("description"), equalTo("test"));
        assertThat(p.getValueFor("parentTaskId"), equalTo(null));
        // when no resource information present
        assertThat(p.getValueFor("resource_stats"), equalTo("{}"));
        assertThat(p.getValueFor("metadata"), equalTo("test_metadata"));

        task.startThreadResourceTracking(
            0,
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(ResourceStats.MEMORY, 0L),
            new ResourceUsageMetric(ResourceStats.CPU, 0L)
        );
        task.updateThreadResourceStats(
            0,
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(ResourceStats.MEMORY, 100),
            new ResourceUsageMetric(ResourceStats.CPU, 100)
        );
        assertThat(
            p.getValueFor("resource_stats"),
            equalTo("{0=[{cpu_time_in_nanos=100, memory_in_bytes=100}, stats_type=worker_stats, is_active=true, threadId=0]}")
        );
    }
}
